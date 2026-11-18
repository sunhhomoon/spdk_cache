/*   SPDX-License-Identifier: BSD-3-Clause
 *
 *   wtcache_cache.c - write-back cache core for the wtcache vbdev (설명은 헤더 참고)
 */

#include "wtcache_cache.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

/* ------------------------------------------------------------------ */
/* spin 방식 reader-writer 락                                          */
/* SPDK reactor 스레드는 잠들면 안 되므로 pthread_rwlock(futex) 대신 돌면서 기다린다. */
/*   bit31: writer가 잡고 있음, bit30: writer가 기다리는 중, 나머지: reader 수   */
/* writer가 기다리기 시작하면 새 reader는 들어오지 못한다 (writer 굶주림 방지). */
/* ------------------------------------------------------------------ */

#define WTC_RW_HELD	(1u << 31)
#define WTC_RW_WAIT	(1u << 30)
#define WTC_RW_READERS	(WTC_RW_WAIT - 1)

#if defined(__x86_64__) || defined(__i386__)
#define wtc_cpu_relax() __builtin_ia32_pause()
#elif defined(__aarch64__)
#define wtc_cpu_relax() __asm__ __volatile__("yield" ::: "memory")
#else
#define wtc_cpu_relax() do { } while (0)
#endif

#ifdef DEBUG
#define WTC_STAT_ADD(field, n) __atomic_fetch_add(&(field), (n), __ATOMIC_RELAXED)
#else
#define WTC_STAT_ADD(field, n) ((void)0)
#endif
#define WTC_STAT_GET(field) __atomic_load_n(&(field), __ATOMIC_RELAXED)

typedef struct {
	uint32_t v;
} wtc_rwlock_t;

static inline void
wtc_rd_lock(wtc_rwlock_t *l)
{
	for (;;) {
		uint32_t v = __atomic_load_n(&l->v, __ATOMIC_RELAXED);

		if ((v & (WTC_RW_HELD | WTC_RW_WAIT)) == 0 &&
		    __atomic_compare_exchange_n(&l->v, &v, v + 1, true,
						__ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
			return;
		}
		wtc_cpu_relax();
	}
}

static inline void
wtc_rd_unlock(wtc_rwlock_t *l)
{
	__atomic_fetch_sub(&l->v, 1, __ATOMIC_RELEASE);
}

static inline void
wtc_wr_lock(wtc_rwlock_t *l)
{
	for (;;) {
		uint32_t v = __atomic_load_n(&l->v, __ATOMIC_RELAXED);

		if ((v & (WTC_RW_HELD | WTC_RW_READERS)) == 0) {
			/* reader도 writer도 없음: WAIT 비트는 지우면서 잡는다
			 * (다른 대기 writer는 다음 반복에서 다시 세운다) */
			if (__atomic_compare_exchange_n(&l->v, &v, WTC_RW_HELD, true,
							__ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
				return;
			}
			continue;
		}
		if ((v & WTC_RW_WAIT) == 0) {
			__atomic_fetch_or(&l->v, WTC_RW_WAIT, __ATOMIC_RELAXED);
		}
		wtc_cpu_relax();
	}
}

static inline void
wtc_wr_unlock(wtc_rwlock_t *l)
{
	__atomic_fetch_and(&l->v, ~WTC_RW_HELD, __ATOMIC_RELEASE);
}

#define WTC_NIL			UINT32_MAX
#define WTC_MAX_RETRIES		3
#define WTC_DEFAULT_SHARDS	64
#define WTC_DEFAULT_REGION	(64 * 1024)	/* bytes */
#define WTC_WB_SCAN_MAX		128	/* wb_next 한 번에 샤드당 살펴볼 dirty extent 수 */

enum wtc_state {
	EXT_DIRTY,	/* dirty FIFO에 있음 */
	EXT_INFLIGHT,	/* writeback 중 */
	EXT_CLEAN,	/* 장치와 같음, clean FIFO에 있음 (축출 가능) */
};

struct wtc_shard;

struct wtc_extent {
	struct wtc_wb_io	io;	/* 반드시 첫 필드 (wtc_wb_io* <-> extent* 변환) */
	uint64_t		seq;	/* 쓰기 순서 번호 */
	uint64_t		live;	/* 인덱스에서 아직 이 extent를 가리키는 블록 수 */
	struct wtc_shard	*shard;
	uint8_t			state;
	uint8_t			retries;
	TAILQ_ENTRY(wtc_extent)	link;	/* dirty FIFO, writeback 중 리스트, clean FIFO 중 하나 */
	struct iovec		iov[];	/* 4KiB 청크들 */
};
TAILQ_HEAD(wtc_ext_list, wtc_extent);

/* 블록 인덱스 엔트리 (체이닝 해시, 인덱스로 연결) */
struct wtc_ment {
	uint64_t		lba;
	struct wtc_extent	*ext;
	uint32_t		next;
};

struct wtc_shard {
	wtc_rwlock_t		lock;	/* 읽기(조회+복사)는 공유, 나머지는 배타 */

	/* 데이터 청크 */
	char			*base;
	uint32_t		nchunks;
	uint32_t		nfree;
	uint32_t		*free_stack;

	/* 블록 인덱스 */
	uint32_t		*buckets;
	uint32_t		bucket_bits;
	struct wtc_ment		*ents;
	uint32_t		ent_free;

	struct wtc_ext_list	dirty;		/* seq 오름차순 */
	struct wtc_ext_list	clean;		/* 장치에 내려간 순서. 앞쪽부터 축출 (FIFO) */
	struct wtc_ext_list	inflight;	/* writeback 중 (LBA가 겹치지 않으면 여러 개) */
	uint32_t		ndirty;		/* lock 없이 엿보기용 (atomic read) */

	/* 통계: 쓰기 쪽은 배타 락 안에서, 읽기 쪽 두 개는 공유 락 아래라 relaxed atomic */
#ifdef DEBUG
	uint64_t		read_blocks, read_hit_blocks;
	uint64_t		write_blocks, write_nomem;
	uint64_t		evictions, writebacks, wb_errors, wb_dropped;
#endif
} __attribute__((aligned(64)));

struct wtc_cache {
	uint32_t		blocklen;
	uint32_t		nshards;
	uint32_t		region_blocks;
	struct wtc_shard	*shards;
	uint64_t		seq;		/* atomic */
	uint64_t		pending;	/* atomic */
	int			error;		/* atomic */
	uint32_t		wb_cursor;	/* writeback 스레드 전용 */
};

/* ------------------------------------------------------------------ */
/* iovec 유틸                                                          */
/* ------------------------------------------------------------------ */

/* iov 배열에서 off 바이트 위치의 주소와, 그 조각 안에서 연속으로 남은 길이 */
static char *
iov_at(const struct iovec *iov, int cnt, size_t off, size_t *avail)
{
	for (int i = 0; i < cnt; i++) {
		if (off < iov[i].iov_len) {
			*avail = iov[i].iov_len - off;
			return (char *)iov[i].iov_base + off;
		}
		off -= iov[i].iov_len;
	}
	*avail = 0;
	return NULL;
}

/* src의 soff 위치부터 len 바이트를 dst의 doff 위치로 복사 (양쪽 다 조각난 버퍼) */
static void
iov_copy(const struct iovec *dst, int dcnt, size_t doff,
	 const struct iovec *src, int scnt, size_t soff, size_t len)
{
	while (len > 0) {
		size_t davail, savail, n;
		char *d = iov_at(dst, dcnt, doff, &davail);
		char *s = iov_at(src, scnt, soff, &savail);

		if (d == NULL || s == NULL) {
			return;
		}
		/* 양쪽 조각 중 먼저 끝나는 곳까지만 한 번에 복사 */
		n = len;
		if (n > davail) {
			n = davail;
		}
		if (n > savail) {
			n = savail;
		}
		memcpy(d, s, n);
		doff += n;
		soff += n;
		len -= n;
	}
}

/* ------------------------------------------------------------------ */
/* 블록 인덱스 (샤드 lock 안에서만 호출)                                */
/* ------------------------------------------------------------------ */

static inline uint32_t
hash_lba(uint64_t lba, uint32_t bits)
{
	return (uint32_t)((lba * 0x9E3779B97F4A7C15ULL) >> (64 - bits));
}

static struct wtc_extent *
map_get(struct wtc_shard *s, uint64_t lba)
{
	uint32_t i = s->buckets[hash_lba(lba, s->bucket_bits)];

	while (i != WTC_NIL) {
		if (s->ents[i].lba == lba) {
			return s->ents[i].ext;
		}
		i = s->ents[i].next;
	}
	return NULL;
}

/* lba가 ext를 가리키게 한다. 이전에 가리키던 extent를 반환 (없으면 NULL) */
static struct wtc_extent *
map_set(struct wtc_shard *s, uint64_t lba, struct wtc_extent *ext)
{
	uint32_t *head = &s->buckets[hash_lba(lba, s->bucket_bits)];
	uint32_t i = *head;
	struct wtc_extent *old;

	while (i != WTC_NIL) {
		if (s->ents[i].lba == lba) {
			old = s->ents[i].ext;
			s->ents[i].ext = ext;
			return old;
		}
		i = s->ents[i].next;
	}
	/* 엔트리 수 <= 샤드 청크가 담을 수 있는 블록 수 이므로 항상 남아 있다 */
	i = s->ent_free;
	s->ent_free = s->ents[i].next;
	s->ents[i].lba = lba;
	s->ents[i].ext = ext;
	s->ents[i].next = *head;
	*head = i;
	return NULL;
}

/* lba가 ext를 가리킬 때만 인덱스에서 지운다 */
static void
map_del_if(struct wtc_shard *s, uint64_t lba, struct wtc_extent *ext)
{
	uint32_t *pp = &s->buckets[hash_lba(lba, s->bucket_bits)];

	while (*pp != WTC_NIL) {
		struct wtc_ment *m = &s->ents[*pp];

		if (m->lba == lba) {
			if (m->ext == ext) {
				uint32_t i = *pp;

				*pp = m->next;
				m->next = s->ent_free;
				s->ent_free = i;
			}
			return;
		}
		pp = &m->next;
	}
}

/* ------------------------------------------------------------------ */
/* extent 관리 (샤드 lock 안에서만 호출)                                */
/* ------------------------------------------------------------------ */

static void
ext_free(struct wtc_shard *s, struct wtc_extent *e)
{
	for (int i = 0; i < e->io.iovcnt; i++) {
		s->free_stack[s->nfree++] =
			(uint32_t)(((char *)e->iov[i].iov_base - s->base) / WTC_CHUNK_SIZE);
	}
	free(e);
}

static void
ext_unmap(struct wtc_shard *s, struct wtc_extent *e)
{
	for (uint64_t b = 0; b < e->io.nblocks; b++) {
		map_del_if(s, e->io.lba + b, e);
	}
}

/* clean extent 축출 (가장 먼저 장치에 내려간 것부터) */
static void
ext_evict(struct wtc_shard *s, struct wtc_extent *e)
{
	ext_unmap(s, e);
	TAILQ_REMOVE(&s->clean, e, link);
	ext_free(s, e);
        WTC_STAT_ADD(s->evictions, 1);
}

/* 블록 하나가 더 새로운 extent로 대체됨 */
static void
ext_unref(struct wtc_shard *s, struct wtc_extent *e)
{
	if (--e->live == 0 && e->state == EXT_CLEAN) {
		TAILQ_REMOVE(&s->clean, e, link);
		ext_free(s, e);
	}
	/* dirty/inflight면 live가 0이어도 장치에 내려쓴 뒤에 해제한다.
	 * (Flush 이전 데이터를 건너뛰면 Flush 의미가 깨질 수 있음) */
}

static inline bool
ext_overlap(const struct wtc_extent *a, const struct wtc_extent *b)
{
	return a->io.lba < b->io.lba + b->io.nblocks && b->io.lba < a->io.lba + a->io.nblocks;
}

/* dirty FIFO를 seq 오름차순으로 유지하며 다시 넣는다 (실패/되돌리기 때) */
static void
dirty_insert_sorted(struct wtc_shard *s, struct wtc_extent *e)
{
	struct wtc_extent *it;

	TAILQ_FOREACH(it, &s->dirty, link) {
		if (it->seq > e->seq) {
			TAILQ_INSERT_BEFORE(it, e, link);
			goto out;
		}
	}
	TAILQ_INSERT_TAIL(&s->dirty, e, link);
out:
	__atomic_store_n(&s->ndirty, s->ndirty + 1, __ATOMIC_RELAXED);
}

static inline struct wtc_shard *
shard_of(struct wtc_cache *c, uint64_t lba)
{
	return &c->shards[(lba / c->region_blocks) % c->nshards];
}

/* lba부터 같은 구역 안에서 처리할 수 있는 블록 수 */
static inline uint64_t
piece_len(struct wtc_cache *c, uint64_t lba, uint64_t remain)
{
	uint64_t to_end = c->region_blocks - (lba % c->region_blocks);

	return remain < to_end ? remain : to_end;
}

/* ------------------------------------------------------------------ */
/* 생성/해제                                                           */
/* ------------------------------------------------------------------ */

static void
shard_fini(struct wtc_shard *s)
{
	struct wtc_extent *e;

	while ((e = TAILQ_FIRST(&s->dirty))) {
		TAILQ_REMOVE(&s->dirty, e, link);
		free(e);
	}
	while ((e = TAILQ_FIRST(&s->clean))) {
		TAILQ_REMOVE(&s->clean, e, link);
		free(e);
	}
	while ((e = TAILQ_FIRST(&s->inflight))) {
		TAILQ_REMOVE(&s->inflight, e, link);
		free(e);
	}
	free(s->free_stack);
	free(s->buckets);
	free(s->ents);
	}

struct wtc_cache *
wtc_create(const struct wtc_opts *opts)
{
	struct wtc_cache *c;
	uint32_t nshards = opts->nshards ? opts->nshards : WTC_DEFAULT_SHARDS;
	uint32_t bl = opts->blocklen;
	uint64_t total_chunks, per_shard;
	uint32_t bpc, region_chunks;

	if (opts->buf == NULL || bl == 0 || bl > WTC_CHUNK_SIZE || WTC_CHUNK_SIZE % bl != 0) {
		return NULL;
	}
	bpc = WTC_CHUNK_SIZE / bl;

	c = calloc(1, sizeof(*c));
	if (c == NULL) {
		return NULL;
	}
	c->blocklen = bl;
	c->nshards = nshards;
	c->region_blocks = opts->region_blocks ? opts->region_blocks : WTC_DEFAULT_REGION / bl;

	total_chunks = opts->buf_size / WTC_CHUNK_SIZE;
	per_shard = total_chunks / nshards;
	region_chunks = (uint32_t)(((uint64_t)c->region_blocks * bl + WTC_CHUNK_SIZE - 1) / WTC_CHUNK_SIZE);
	/* 구역 하나 크기의 쓰기 조각은 언제나 샤드에 들어갈 수 있어야 한다 */
	if (per_shard < region_chunks || per_shard * bpc >= WTC_NIL) {
		free(c);
		return NULL;
	}

	c->shards = aligned_alloc(64, sizeof(struct wtc_shard) * nshards);
	if (c->shards == NULL) {
		free(c);
		return NULL;
	}
	memset(c->shards, 0, sizeof(struct wtc_shard) * nshards);

	for (uint32_t i = 0; i < nshards; i++) {
		struct wtc_shard *s = &c->shards[i];
		uint32_t nents = (uint32_t)(per_shard * bpc);
		uint32_t bits = 1;

		while ((1ULL << bits) < nents) {
			bits++;
		}
		s->lock.v = 0;
		TAILQ_INIT(&s->dirty);
		TAILQ_INIT(&s->clean);
		TAILQ_INIT(&s->inflight);
		s->base = (char *)opts->buf + (uint64_t)i * per_shard * WTC_CHUNK_SIZE;
		s->nchunks = (uint32_t)per_shard;
		s->nfree = s->nchunks;
		s->free_stack = malloc(sizeof(uint32_t) * s->nchunks);
		s->bucket_bits = bits;
		s->buckets = malloc(sizeof(uint32_t) << bits);
		s->ents = malloc(sizeof(struct wtc_ment) * nents);
		if (!s->free_stack || !s->buckets || !s->ents) {
			for (uint32_t j = 0; j <= i; j++) {
				shard_fini(&c->shards[j]);
			}
			free(c->shards);
			free(c);
			return NULL;
		}
		for (uint32_t k = 0; k < s->nchunks; k++) {
			s->free_stack[k] = s->nchunks - 1 - k;
		}
		memset(s->buckets, 0xff, sizeof(uint32_t) << bits);	/* WTC_NIL */
		for (uint32_t k = 0; k < nents; k++) {
			s->ents[k].next = (k + 1 < nents) ? k + 1 : WTC_NIL;
		}
		s->ent_free = 0;
	}
	return c;
}

void
wtc_destroy(struct wtc_cache *c)
{
	if (c == NULL) {
		return;
	}
	for (uint32_t i = 0; i < c->nshards; i++) {
		shard_fini(&c->shards[i]);
	}
	free(c->shards);
	free(c);
}

/* ------------------------------------------------------------------ */
/* I/O 경로                                                            */
/* ------------------------------------------------------------------ */

static int
write_piece(struct wtc_cache *c, uint64_t lba, uint64_t n,
	    const struct iovec *iovs, int iovcnt, size_t src_off)
{
	struct wtc_shard *s = shard_of(c, lba);
	size_t bytes = n * c->blocklen;
	uint32_t need = (uint32_t)((bytes + WTC_CHUNK_SIZE - 1) / WTC_CHUNK_SIZE);
	struct wtc_extent *e, *victim, *old;

	/* malloc은 lock 밖에서 */
	e = malloc(sizeof(*e) + need * sizeof(struct iovec));
	if (e == NULL) {
		return -ENOMEM;
	}

	wtc_wr_lock(&s->lock);

	while (s->nfree < need && (victim = TAILQ_FIRST(&s->clean)) != NULL) {
		ext_evict(s, victim);
	}
	if (s->nfree < need) {
		/* 전부 dirty/inflight: writeback이 따라잡을 때까지 재시도 필요 */
        	WTC_STAT_ADD(s->write_nomem, 1);
		wtc_wr_unlock(&s->lock);
		free(e);
		return -ENOMEM;
	}

	for (uint32_t i = 0; i < need; i++) {
		size_t left = bytes - (size_t)i * WTC_CHUNK_SIZE;

		e->iov[i].iov_base = s->base + (size_t)s->free_stack[--s->nfree] * WTC_CHUNK_SIZE;
		e->iov[i].iov_len = left < WTC_CHUNK_SIZE ? left : WTC_CHUNK_SIZE;
	}
	wtc_wr_unlock(&s->lock);

	/* 청크는 확보했지만 아직 인덱스/리스트 어디에도 없어서 다른 스레드가 볼 수 없다.
	 * 그러니 데이터 복사는 락 없이 한다. */
	e->io.lba = lba;
	e->io.nblocks = n;
	e->io.iovs = e->iov;
	e->io.iovcnt = (int)need;
	e->shard = s;
	e->state = EXT_DIRTY;
	e->retries = 0;
	e->live = n;
	iov_copy(e->iov, (int)need, 0, iovs, iovcnt, src_off, bytes);

	/* 이제 공개: 인덱스와 dirty FIFO에 넣는다 */
	wtc_wr_lock(&s->lock);
	for (uint64_t b = 0; b < n; b++) {
		old = map_set(s, lba + b, e);
		if (old != NULL) {
			ext_unref(s, old);
		}
	}

	/* seq는 lock 안에서 매겨야 샤드 FIFO가 seq 순서가 된다 */
	e->seq = __atomic_add_fetch(&c->seq, 1, __ATOMIC_SEQ_CST);
	TAILQ_INSERT_TAIL(&s->dirty, e, link);
	__atomic_store_n(&s->ndirty, s->ndirty + 1, __ATOMIC_RELAXED);
        WTC_STAT_ADD(s->write_blocks, n);
	__atomic_add_fetch(&c->pending, 1, __ATOMIC_RELAXED);

	wtc_wr_unlock(&s->lock);
	return 0;
}

int
wtc_write(struct wtc_cache *c, uint64_t lba, uint64_t nblocks,
	  const struct iovec *iovs, int iovcnt)
{
	uint64_t done = 0;

	while (done < nblocks) {
		uint64_t n = piece_len(c, lba + done, nblocks - done);
		int rc = write_piece(c, lba + done, n, iovs, iovcnt, done * c->blocklen);

		if (rc != 0) {
			return rc;
		}
		done += n;
	}
	return 0;
}

static int
add_run(struct wtc_run *runs, int nruns, int max_runs, uint64_t lba, uint64_t n)
{
	if (nruns > 0 && runs[nruns - 1].lba + runs[nruns - 1].nblocks == lba) {
		runs[nruns - 1].nblocks += n;
		return nruns;
	}
	if (nruns >= max_runs) {
		return -1;
	}
	runs[nruns].lba = lba;
	runs[nruns].nblocks = n;
	return nruns + 1;
}

int
wtc_read(struct wtc_cache *c, uint64_t lba, uint64_t nblocks,
	 const struct iovec *iovs, int iovcnt, struct wtc_run *runs, int max_runs)
{
	uint32_t bl = c->blocklen;
	uint64_t done = 0;
	int nruns = 0;

	while (done < nblocks) {
		uint64_t cur = lba + done;
		uint64_t pn = piece_len(c, cur, nblocks - done);
		struct wtc_shard *s = shard_of(c, cur);
		uint64_t j = 0;

		wtc_rd_lock(&s->lock);
		while (j < pn) {
			struct wtc_extent *e = map_get(s, cur + j);
			uint64_t k = j + 1;

			if (e != NULL) {
				/* 같은 extent에 연속으로 있는 블록은 한 번에 복사 */
				while (k < pn && map_get(s, cur + k) == e) {
					k++;
				}
				iov_copy(iovs, iovcnt, (done + j) * bl,
					 e->iov, e->io.iovcnt, (cur + j - e->io.lba) * bl, (k - j) * bl);
        			WTC_STAT_ADD(s->read_hit_blocks, k - j);
			} else {
				while (k < pn && map_get(s, cur + k) == NULL) {
					k++;
				}
				nruns = add_run(runs, nruns, max_runs, cur + j, k - j);
				if (nruns < 0) {
					wtc_rd_unlock(&s->lock);
					return -ENOSPC;	/* max_runs가 WTC_MAX_RUNS보다 작음 */
				}
			}
			j = k;
		}
        	WTC_STAT_ADD(s->read_blocks, pn);
		wtc_rd_unlock(&s->lock);
		done += pn;
	}
	return nruns;
}

/* ------------------------------------------------------------------ */
/* writeback / flush (writeback 스레드 전용)                           */
/* ------------------------------------------------------------------ */

int
wtc_wb_next(struct wtc_cache *c, struct wtc_wb_io **out, int max)
{
	int k = 0;
	uint32_t t;

	for (t = 0; t < c->nshards && k < max; t++) {
		struct wtc_shard *s = &c->shards[(c->wb_cursor + t) % c->nshards];
		struct wtc_extent *e;

		if (__atomic_load_n(&s->ndirty, __ATOMIC_RELAXED) == 0) {
			continue;
		}
		wtc_wr_lock(&s->lock);
		{
			/* 앞(오래된 것)부터 보면서, writeback 중인 것이나 앞에서 건너뛴 것과
			 * LBA가 겹치면 건너뛴다. 겹치는 쌍은 항상 오래된 쪽이 먼저 끝나야
			 * 옛 데이터가 새 데이터를 덮지 않는다. 순차 쓰기는 거의 안 겹친다. */
			struct wtc_extent *skipped[WTC_WB_SCAN_MAX];
			struct wtc_extent *next, *it;
			int nskip = 0, scanned = 0;

			for (e = TAILQ_FIRST(&s->dirty); e != NULL && k < max && scanned < WTC_WB_SCAN_MAX;
			     e = next, scanned++) {
				bool blocked = false;

				next = TAILQ_NEXT(e, link);
				TAILQ_FOREACH(it, &s->inflight, link) {
					if (ext_overlap(e, it)) {
						blocked = true;
						break;
					}
				}
				for (int j = 0; !blocked && j < nskip; j++) {
					blocked = ext_overlap(e, skipped[j]);
				}
				if (blocked) {
					skipped[nskip++] = e;
					continue;
				}
				TAILQ_REMOVE(&s->dirty, e, link);
				__atomic_store_n(&s->ndirty, s->ndirty - 1, __ATOMIC_RELAXED);
				e->state = EXT_INFLIGHT;
				TAILQ_INSERT_TAIL(&s->inflight, e, link);
				out[k++] = &e->io;
			}
		}
		wtc_wr_unlock(&s->lock);
	}
	c->wb_cursor = (c->wb_cursor + t) % c->nshards;	/* 샤드 간 공평하게 */
	return k;
}

void
wtc_wb_done(struct wtc_cache *c, struct wtc_wb_io *io, bool success)
{
	struct wtc_extent *e = (struct wtc_extent *)io;
	struct wtc_shard *s = e->shard;

	wtc_wr_lock(&s->lock);
	TAILQ_REMOVE(&s->inflight, e, link);
	if (success) {
        	WTC_STAT_ADD(s->writebacks, 1);
		if (e->live == 0) {
			ext_free(s, e);		/* 이미 전부 새 데이터로 대체됨 */
		} else {
			e->state = EXT_CLEAN;
			TAILQ_INSERT_TAIL(&s->clean, e, link);
		}
		__atomic_sub_fetch(&c->pending, 1, __ATOMIC_RELAXED);
	} else if (++e->retries >= WTC_MAX_RETRIES) {
		/* 포기: 캐시에서도 지우므로 이후 읽기는 장치의 옛 데이터를 본다 */
        	WTC_STAT_ADD(s->wb_errors, 1);
        	WTC_STAT_ADD(s->wb_dropped, 1);
		ext_unmap(s, e);
		ext_free(s, e);
		__atomic_store_n(&c->error, 1, __ATOMIC_SEQ_CST);
		__atomic_sub_fetch(&c->pending, 1, __ATOMIC_RELAXED);
	} else {
		/* seq 순서 자리에 다시 넣어 재시도 */
        	WTC_STAT_ADD(s->wb_errors, 1);
		e->state = EXT_DIRTY;
		dirty_insert_sorted(s, e);
	}
	wtc_wr_unlock(&s->lock);
}

void
wtc_wb_requeue(struct wtc_cache *c, struct wtc_wb_io *io)
{
	struct wtc_extent *e = (struct wtc_extent *)io;
	struct wtc_shard *s = e->shard;

	(void)c;
	wtc_wr_lock(&s->lock);
	TAILQ_REMOVE(&s->inflight, e, link);
	e->state = EXT_DIRTY;
	dirty_insert_sorted(s, e);
	wtc_wr_unlock(&s->lock);
}

uint64_t
wtc_flush_mark(struct wtc_cache *c)
{
	return __atomic_load_n(&c->seq, __ATOMIC_SEQ_CST);
}

bool
wtc_flush_reached(struct wtc_cache *c, uint64_t mark)
{
	for (uint32_t i = 0; i < c->nshards; i++) {
		struct wtc_shard *s = &c->shards[i];
		struct wtc_extent *e;
		uint64_t oldest = UINT64_MAX;

		wtc_rd_lock(&s->lock);
		TAILQ_FOREACH(e, &s->inflight, link) {
			if (e->seq < oldest) {
				oldest = e->seq;
			}
		}
		e = TAILQ_FIRST(&s->dirty);
		if (e != NULL && e->seq < oldest) {
			oldest = e->seq;
		}
		wtc_rd_unlock(&s->lock);

		if (oldest <= mark) {
			return false;
		}
	}
	return true;
}

bool
wtc_take_error(struct wtc_cache *c)
{
	return __atomic_exchange_n(&c->error, 0, __ATOMIC_SEQ_CST) != 0;
}

uint64_t
wtc_pending(struct wtc_cache *c)
{
	return __atomic_load_n(&c->pending, __ATOMIC_RELAXED);
}

void
wtc_get_stats(struct wtc_cache *c, struct wtc_stats *st)
{
	memset(st, 0, sizeof(*st));
	for (uint32_t i = 0; i < c->nshards; i++) {
		struct wtc_shard *s = &c->shards[i];
#ifdef DEBUG
		st->read_blocks += WTC_STAT_GET(s->read_blocks);
		st->read_hit_blocks += WTC_STAT_GET(s->read_hit_blocks);
		st->write_blocks += WTC_STAT_GET(s->write_blocks);
		st->write_nomem += WTC_STAT_GET(s->write_nomem);
		st->evictions += WTC_STAT_GET(s->evictions);
		st->writebacks += WTC_STAT_GET(s->writebacks);
		st->wb_errors += WTC_STAT_GET(s->wb_errors);
		st->wb_dropped += WTC_STAT_GET(s->wb_dropped);
#endif
		wtc_rd_lock(&s->lock);
		st->used_bytes += (uint64_t)(s->nchunks - s->nfree) * WTC_CHUNK_SIZE;
		wtc_rd_unlock(&s->lock);
		st->total_bytes += (uint64_t)s->nchunks * WTC_CHUNK_SIZE;
	}
	st->pending = wtc_pending(c);
}
