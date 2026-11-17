/*
 * test_wtcache_cache.c - 캐시 엔진 단독 검증 (SPDK 불필요)
 *
 *  - 가짜 디스크 + writeback 스레드 (완료 순서 뒤섞기, 실패 주입)
 *  - 여러 쓰기 스레드가 각자 맡은 LBA 영역에 랜덤 읽기/쓰기/flush
 *  - 읽기 결과가 항상 마지막으로 쓴 데이터인지,
 *    flush 완료 후 디스크가 그 시점까지 쓴 데이터와 같은지 검사
 *
 *  gcc -O1 -g -fsanitize=thread -pthread wtcache_cache.c test_wtcache_cache.c -o t && ./t
 */
#include "wtcache_cache.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BL		512
#define NTHREADS	4
#define BLK_PER_THR	8192		/* 스레드당 4MiB 영역 */
#define DISK_BLOCKS	(NTHREADS * BLK_PER_THR)
#define CACHE_BYTES	(1u << 20)	/* 1MiB: 일부러 작게 해서 축출/ENOMEM 경로를 탄다 */
#define OPS		40000
#define MAX_IO_BLK	48		/* 최대 24KiB, 구역(16KiB) 경계를 자주 넘도록 */

static struct wtc_cache *g_c;
static uint8_t g_disk[(size_t)DISK_BLOCKS * BL];
static pthread_mutex_t g_disk_lock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t g_model[DISK_BLOCKS];		/* 블록별 최신 버전 (해당 스레드만 접근) */
static int g_stop;
static int g_fail_pct = 2;			/* writeback 실패 주입 확률(%) */

static void
fill(uint8_t *p, uint64_t lba, uint32_t ver)
{
	for (int i = 0; i < BL; i += 4) {
		uint32_t v = (uint32_t)(lba * 2654435761u) ^ ver ^ (uint32_t)i;
		memcpy(p + i, &v, 4);
	}
}

static int
check(const uint8_t *p, uint64_t lba, uint32_t ver)
{
	uint8_t exp[BL];

	if (ver == 0) {	/* 한 번도 안 쓴 블록 = 0 */
		memset(exp, 0, BL);
	} else {
		fill(exp, lba, ver);
	}
	return memcmp(p, exp, BL) == 0;
}

/* writeback 스레드: 꺼낸 것들을 뒤섞인 순서로 완료 */
static void *
wb_thread(void *arg)
{
	struct wtc_wb_io *q[256];
	int nq = 0;
	unsigned seed = 1234;

	(void)arg;
	while (!__atomic_load_n(&g_stop, __ATOMIC_ACQUIRE) || wtc_pending(g_c) > 0) {
		nq += wtc_wb_next(g_c, q + nq, 256 - nq);
		if (nq == 0) {
			usleep(50);
			continue;
		}
		/* 아무거나 하나 골라 완료 (NVMe 완료 순서는 보장되지 않음) */
		int i = rand_r(&seed) % nq;
		struct wtc_wb_io *io = q[i];
		bool ok = (rand_r(&seed) % 100) >= g_fail_pct;

		if (ok) {
			pthread_mutex_lock(&g_disk_lock);
			size_t off = io->lba * BL;
			for (int k = 0; k < io->iovcnt; k++) {
				memcpy(g_disk + off, io->iovs[k].iov_base, io->iovs[k].iov_len);
				off += io->iovs[k].iov_len;
			}
			pthread_mutex_unlock(&g_disk_lock);
		}
		wtc_wb_done(g_c, io, ok);
		q[i] = q[--nq];
	}
	return NULL;
}

static long g_errors, g_nomem, g_flushes;

static void *
io_thread(void *arg)
{
	int t = (int)(long)arg;
	uint64_t base = (uint64_t)t * BLK_PER_THR;
	unsigned seed = 99 + t;
	uint8_t *buf = malloc(MAX_IO_BLK * BL);
	struct wtc_run runs[WTC_MAX_RUNS(MAX_IO_BLK)];

	for (int op = 0; op < OPS; op++) {
		int r = rand_r(&seed) % 100;
		uint64_t n = 1 + rand_r(&seed) % MAX_IO_BLK;
		uint64_t lba = base + rand_r(&seed) % (BLK_PER_THR - n);
		/* 버퍼를 iov 여러 개로 나눠서 경계 처리도 검사 */
		struct iovec iov[3];
		size_t len = n * BL, a = len / 3, b = len / 3;
		iov[0] = (struct iovec){ buf, a };
		iov[1] = (struct iovec){ buf + a, b };
		iov[2] = (struct iovec){ buf + a + b, len - a - b };

		if (r < 45) {				/* write */
			uint32_t ver = (uint32_t)op + 1;
			for (uint64_t i = 0; i < n; i++) {
				fill(buf + i * BL, lba + i, ver);
			}
			while (wtc_write(g_c, lba, n, iov, 3) == -ENOMEM) {
				__atomic_add_fetch(&g_nomem, 1, __ATOMIC_RELAXED);
				usleep(100);	/* bdev 계층의 NOMEM 재시도 흉내 */
			}
			for (uint64_t i = 0; i < n; i++) {
				g_model[lba + i] = ver;
			}
		} else if (r < 98) {			/* read */
			memset(buf, 0xEE, len);
			int nr = wtc_read(g_c, lba, n, iov, 3, runs, WTC_MAX_RUNS(n));
			assert(nr >= 0);
			pthread_mutex_lock(&g_disk_lock);
			for (int k = 0; k < nr; k++) {
				memcpy(buf + (runs[k].lba - lba) * BL, g_disk + runs[k].lba * BL,
				       runs[k].nblocks * BL);
			}
			pthread_mutex_unlock(&g_disk_lock);
			for (uint64_t i = 0; i < n; i++) {
				if (!check(buf + i * BL, lba + i, g_model[lba + i])) {
					if (__atomic_add_fetch(&g_errors, 1, __ATOMIC_RELAXED) < 5) {
						fprintf(stderr, "READ MISMATCH thr=%d lba=%lu\n", t,
							(unsigned long)(lba + i));
					}
				}
			}
		} else {				/* flush */
			uint64_t mark = wtc_flush_mark(g_c);
			while (!wtc_flush_reached(g_c, mark)) {
				usleep(50);
			}
			__atomic_add_fetch(&g_flushes, 1, __ATOMIC_RELAXED);
			/* 내 영역은 나만 쓰므로, flush 이후 디스크 = 내 모델이어야 한다 */
			pthread_mutex_lock(&g_disk_lock);
			for (uint64_t i = 0; i < BLK_PER_THR; i++) {
				if (!check(g_disk + (base + i) * BL, base + i, g_model[base + i])) {
					if (__atomic_add_fetch(&g_errors, 1, __ATOMIC_RELAXED) < 5) {
						fprintf(stderr, "FLUSH MISMATCH thr=%d lba=%lu\n", t,
							(unsigned long)(base + i));
					}
				}
			}
			pthread_mutex_unlock(&g_disk_lock);
		}
	}
	free(buf);
	return NULL;
}

int
main(int argc, char **argv)
{
	void *mem = aligned_alloc(4096, CACHE_BYTES);
	struct wtc_opts o = {
		.buf = mem, .buf_size = CACHE_BYTES, .blocklen = BL,
		.nshards = 8, .region_blocks = 32,	/* 16KiB 구역 */
	};
	pthread_t wb, io[NTHREADS];
	struct wtc_stats st;

	if (argc > 1) {
		g_fail_pct = atoi(argv[1]);
	}
	g_c = wtc_create(&o);
	assert(g_c);

	pthread_create(&wb, NULL, wb_thread, NULL);
	for (long t = 0; t < NTHREADS; t++) {
		pthread_create(&io[t], NULL, io_thread, (void *)t);
	}
	for (int t = 0; t < NTHREADS; t++) {
		pthread_join(io[t], NULL);
	}
	__atomic_store_n(&g_stop, 1, __ATOMIC_RELEASE);
	pthread_join(wb, NULL);

	wtc_get_stats(g_c, &st);
	int dropped_err = wtc_take_error(g_c);

	/* 최종: 남은 dirty가 없으므로 디스크 = 모델 */
	for (uint64_t i = 0; i < DISK_BLOCKS; i++) {
		if (!check(g_disk + i * BL, i, g_model[i])) {
			g_errors++;
		}
	}
	printf("fail_pct=%d hit=%lu/%lu writes=%lu nomem=%lu evict=%lu wb=%lu wb_err=%lu dropped=%lu "
	       "flushes=%ld pending=%lu used=%lu/%lu\n",
	       g_fail_pct, st.read_hit_blocks, st.read_blocks, st.write_blocks, st.write_nomem,
	       st.evictions, st.writebacks, st.wb_errors, st.wb_dropped, g_flushes, st.pending,
	       st.used_bytes, st.total_bytes);
	printf("errors=%ld (dropped_flag=%d) -> %s\n", g_errors, dropped_err,
	       g_errors == 0 ? "PASS" : (st.wb_dropped ? "MISMATCH (데이터 버림 발생, 예상된 결과)" : "FAIL"));
	wtc_destroy(g_c);
	free(mem);
	return g_errors && !st.wb_dropped;
}
