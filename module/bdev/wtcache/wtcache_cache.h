/*   SPDX-License-Identifier: BSD-3-Clause
 *
 *   wtcache_cache.h - write-back cache core for the wtcache vbdev
 *
 *   SPDK에 의존하지 않는 순수 C 캐시 엔진. vbdev 쪽에서 아래 API를 호출한다.
 *
 *   - 데이터 단위: 호스트 쓰기 하나 = extent 하나 (blocklen 단위 가변 크기)
 *   - 인덱스: LBA(블록) -> 그 블록의 최신 데이터를 가진 extent
 *   - 샤딩: LBA를 region_blocks 단위 구역으로 나누고, 구역마다 샤드를 정한다.
 *           extent는 구역 경계를 넘지 않게 쪼개 저장하므로 한 extent는 항상 한 샤드 소속.
 *           샤드마다 락/메모리/인덱스/리스트가 따로 있어 락 경합이 적다.
 *           락은 reader-writer 방식: 읽기(인덱스 조회+복사)끼리는 서로 막지 않는다.
 *   - 순서: 샤드마다 dirty FIFO를 seq 순으로 보며, writeback 중이거나 앞에 남아 있는
 *           extent와 LBA가 겹치지 않는 것은 동시에 여러 개 내보낸다.
 *           겹치는 쌍은 항상 오래된 쪽이 끝난 뒤에 새 쪽이 나가므로 옛 데이터가 새 데이터를 덮지 않는다.
 *   - 공간이 없으면 clean extent를 FIFO로(장치에 먼저 내려간 것부터) 축출하고,
 *     그래도 없으면 -ENOMEM. 읽기는 축출 순서에 영향을 주지 않는다.
 *     (vbdev는 bdev_io를 SPDK_BDEV_IO_STATUS_NOMEM으로 완료 -> bdev 계층이 알아서 재시도)
 *   - Flush: wtc_flush_mark()로 표시한 시점까지의 쓰기가 전부 장치에 내려가면
 *     wtc_flush_reached()가 true.
 *
 *   스레드: wtc_write/wtc_read는 아무 스레드에서나 동시에 불러도 된다.
 *          wtc_wb_next/wtc_wb_done/wtc_flush_reached는 writeback 스레드 하나에서 부르는 것을 가정.
 */

#ifndef WTCACHE_CACHE_H
#define WTCACHE_CACHE_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/uio.h>

#define WTC_CHUNK_SIZE 4096u

struct wtc_cache;

struct wtc_opts {
	void		*buf;		/* 캐시 데이터 메모리. SPDK에서는 spdk_dma_zmalloc()으로 할당 */
	uint64_t	buf_size;	/* 바이트 */
	uint32_t	blocklen;	/* base bdev 블록 크기 (4096의 약수) */
	uint32_t	nshards;	/* 0이면 64 */
	uint32_t	region_blocks;	/* 한 샤드에 연속으로 배정되는 블록 수. 0이면 64KiB 분량 */
};

/* writeback 한 건: 이 범위를 iovs 내용으로 base에 쓰면 된다 (LBA/개수는 blocklen 단위) */
struct wtc_wb_io {
	uint64_t	lba;
	uint64_t	nblocks;
	struct iovec	*iovs;
	int		iovcnt;
	void		*priv;		/* 호출자 자유 사용 (캐시는 건드리지 않음) */
};

/* 읽기에서 캐시에 없어 base에서 읽어야 하는 구간 */
struct wtc_run {
	uint64_t	lba;
	uint64_t	nblocks;
};
#define WTC_MAX_RUNS(nblocks) (((nblocks) + 1) / 2)

struct wtc_stats {
	uint64_t read_blocks;
	uint64_t read_hit_blocks;
	uint64_t write_blocks;
	uint64_t write_nomem;	/* 공간 부족으로 -ENOMEM 반환한 횟수 */
	uint64_t evictions;
	uint64_t writebacks;
	uint64_t wb_errors;
	uint64_t wb_dropped;	/* 재시도 초과로 버린 extent 수 (데이터 유실) */
	uint64_t pending;	/* dirty + writeback 중인 extent 수 */
	uint64_t used_bytes;
	uint64_t total_bytes;
};

struct wtc_cache *wtc_create(const struct wtc_opts *opts);
void wtc_destroy(struct wtc_cache *c);	/* buf는 호출자가 해제 */

/*
 * 쓰기: 데이터를 캐시에 복사하고 dirty로 등록한다.
 * 0: 캐시됨 (바로 완료 응답해도 됨), -ENOMEM: 공간 없음 (나중에 재시도).
 * 구역 경계를 넘는 쓰기는 조각으로 나뉘며, -ENOMEM이면 일부 조각만 들어갔을 수 있다.
 * 같은 데이터로 다시 쓰면 그대로 덮어쓰므로 전체 재시도해도 결과는 같다.
 */
int wtc_write(struct wtc_cache *c, uint64_t lba, uint64_t nblocks,
	      const struct iovec *iovs, int iovcnt);

/*
 * 읽기: 캐시에 있는 블록은 iovs의 해당 위치에 바로 복사하고,
 * 없는 구간을 runs[]에 담아 개수를 반환한다 (0 = 전부 히트).
 * runs는 WTC_MAX_RUNS(nblocks)개 이상이어야 한다.
 * 반환된 구간을 base에서 iovs의 같은 위치로 읽어 오면 요청이 완성된다.
 */
int wtc_read(struct wtc_cache *c, uint64_t lba, uint64_t nblocks,
	     const struct iovec *iovs, int iovcnt, struct wtc_run *runs, int max_runs);

/*
 * FUA(바로 영속화) 쓰기용.
 * wtc_range_dirty(): 범위 안에 아직 SSD에 안 내려간 캐시 데이터가 있으면 true.
 *   false면 그 쓰기는 캐시를 거치지 않고 바로 SSD에 써도 순서 문제가 없다.
 * wtc_insert_clean(): SSD 쓰기가 끝난 데이터를 clean으로 캐시에 넣는다 (읽기 히트용).
 *   자리가 없으면 넣지 않고 그 범위의 옛 clean 데이터만 지운다.
 */
bool wtc_range_dirty(struct wtc_cache *c, uint64_t lba, uint64_t nblocks);
void wtc_insert_clean(struct wtc_cache *c, uint64_t lba, uint64_t nblocks,
		      const struct iovec *iovs, int iovcnt);

/* writeback할 extent를 최대 max개 꺼낸다 (LBA가 겹치지 않는 것끼리는 동시에). 반환 개수 */
int wtc_wb_next(struct wtc_cache *c, struct wtc_wb_io **out, int max);
/* base 쓰기 완료 시 호출 */
void wtc_wb_done(struct wtc_cache *c, struct wtc_wb_io *io, bool success);
/* 꺼냈지만 제출 자체를 못 했을 때(-ENOMEM 등) 실패 횟수를 세지 않고 되돌린다 */
void wtc_wb_requeue(struct wtc_cache *c, struct wtc_wb_io *io);

/* Flush 도착 시 호출해 표시값을 받고, wtc_flush_reached()가 true가 되면 그 이전 쓰기는 모두 장치에 있음 */
uint64_t wtc_flush_mark(struct wtc_cache *c);
bool wtc_flush_reached(struct wtc_cache *c, uint64_t mark);
/* writeback 실패로 데이터를 버린 적이 있으면 true를 반환하고 표시를 지운다 (Flush를 FAILED로) */
bool wtc_take_error(struct wtc_cache *c);

uint64_t wtc_pending(struct wtc_cache *c);	/* dirty + writeback 중 extent 수 */
void wtc_get_stats(struct wtc_cache *c, struct wtc_stats *st);

#endif /* WTCACHE_CACHE_H */
