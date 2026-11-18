/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/*
 * This is a simple example of a virtual block device module that passes IO
 * down to a bdev (or bdevs) that its configured to attach to.
 */

#include "spdk/stdinc.h"

#include "vbdev_wtcache.h"
#include "wtcache_cache.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

/* [wtcache] 캐시 설정 (일단 상수) */
#ifndef WTCACHE_CACHE_MB
#define WTCACHE_CACHE_MB	1024	/* DRAM 캐시 크기 (빌드 시 -DWTCACHE_CACHE_MB=로 변경 가능) */
#endif
#define WTCACHE_WB_MAX_INFLIGHT	64	/* 동시에 내보낼 writeback 수 */
#define WTCACHE_WB_POLL_US	100	/* writeback poller 주기 */

/* This namespace UUID was generated using uuid_generate() method. */
#define BDEV_WTCACHE_NAMESPACE_UUID "7e25812e-c8c0-4d3f-8599-16d790555b85"

static int vbdev_wtcache_init(void);
static int vbdev_wtcache_get_ctx_size(void);
static void vbdev_wtcache_examine(struct spdk_bdev *bdev);
static void vbdev_wtcache_finish(void);
static int vbdev_wtcache_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module wtcache_if = {
	.name = "wtcache",
	.module_init = vbdev_wtcache_init,
	.get_ctx_size = vbdev_wtcache_get_ctx_size,
	.examine_config = vbdev_wtcache_examine,
	.module_fini = vbdev_wtcache_finish,
	.config_json = vbdev_wtcache_config_json
};

SPDK_BDEV_MODULE_REGISTER(wtcache, &wtcache_if)

/* List of pt_bdev names and their base bdevs via configuration file.
 * Used so we can parse the conf once at init and use this list in examine().
 */
struct bdev_names {
	char			*vbdev_name;
	char			*bdev_name;
	struct spdk_uuid	uuid;
	TAILQ_ENTRY(bdev_names)	link;
};
static TAILQ_HEAD(, bdev_names) g_bdev_names = TAILQ_HEAD_INITIALIZER(g_bdev_names);

/* List of virtual bdevs and associated info for each. */
struct vbdev_wtcache {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc		*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		pt_bdev;    /* the PT virtual bdev */
	TAILQ_ENTRY(vbdev_wtcache)	link;
	struct spdk_thread		*thread;    /* thread where base device is opened */

	/* [wtcache] 캐시 */
	struct wtc_cache		*cache;
	void				*cache_mem;	/* spdk_dma_zmalloc 메모리 */

	/* [wtcache] writeback 스레드 (아래 필드들은 wb_thread에서만 만진다) */
	struct spdk_thread		*wb_thread;
	struct spdk_io_channel		*wb_ch;		/* wb_thread용 base 채널 */
	struct spdk_poller		*wb_poller;
	int				wb_inflight;
	bool				draining;
	TAILQ_HEAD(, wtcache_bdev_io)	flushes;	/* 대기 중인 Flush */
	bool				kick_pending;	/* atomic: 중복 깨우기 방지 */
};
static TAILQ_HEAD(, vbdev_wtcache) g_pt_nodes = TAILQ_HEAD_INITIALIZER(g_pt_nodes);

/* The pt vbdev channel struct. It is allocated and freed on my behalf by the io channel code.
 * If this vbdev needed to implement a poller or a queue for IO, this is where those things
 * would be defined. This wtcache bdev doesn't actually need to allocate a channel, it could
 * simply pass back the channel of the bdev underneath it but for example purposes we will
 * present its own to the upper layers.
 */
struct pt_io_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */
};

/* Just for fun, this pt_bdev module doesn't need it but this is essentially a per IO
 * context that we get handed by the bdev layer.
 */
struct wtcache_bdev_io {
	uint8_t test;

	/* bdev related */
	struct spdk_io_channel *ch;

	/* for bdev_io_wait */
	struct spdk_bdev_io_wait_entry bdev_io_wait;

	/* [wtcache] */
	struct spdk_bdev_io		*bdev_io;	/* 자기 자신을 가리키는 역포인터 */
	struct vbdev_wtcache		*node;
	TAILQ_ENTRY(wtcache_bdev_io)	link;		/* Flush 대기 리스트 */
	uint64_t			flush_mark;
	bool				failed;
	uint32_t			read_remaining;	/* 부분 히트 읽기에서 남은 base 읽기 수 */
};

static void vbdev_wtcache_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);


/* Callback for unregistering the IO device. */
static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_wtcache *pt_node  = io_device;

	/* Done with this pt_node. */
	wtc_destroy(pt_node->cache);		/* [wtcache] */
	spdk_dma_free(pt_node->cache_mem);	/* [wtcache] */
	free(pt_node->pt_bdev.name);
	free(pt_node);
}

/* [wtcache] 원래 destruct가 하던 정리. base를 연 스레드(pt_node->thread)에서 실행 */
static void
wtcache_destruct_finish(void *ctx)
{
	struct vbdev_wtcache *pt_node = ctx;

	spdk_bdev_module_release_bdev(pt_node->base_bdev);
	spdk_bdev_close(pt_node->base_desc);
	spdk_io_device_unregister(pt_node, _device_unregister_cb);
	spdk_bdev_destruct_done(&pt_node->pt_bdev, 0);
}

static void wtcache_wb_drain_start(void *ctx);

/* Called after we've unregistered following a hot remove callback.
 * [wtcache] dirty 데이터를 장치에 다 내린 뒤 정리해야 하므로 비동기(1 반환)로 끝낸다.
 */
static int
vbdev_wtcache_destruct(void *ctx)
{
	struct vbdev_wtcache *pt_node = (struct vbdev_wtcache *)ctx;

	TAILQ_REMOVE(&g_pt_nodes, pt_node, link);

	if (pt_node->wb_thread != NULL) {
		/* drain이 끝나면 wb 스레드가 wtcache_destruct_finish를 부른다 */
		spdk_thread_send_msg(pt_node->wb_thread, wtcache_wb_drain_start, pt_node);
	} else {
		SPDK_ERRLOG("%s: no writeback thread, %lu dirty extents lost\n",
			    pt_node->pt_bdev.name, wtc_pending(pt_node->cache));
		spdk_thread_send_msg(pt_node->thread, wtcache_destruct_finish, pt_node);
	}
	return 1;
}

/* Completion callback for IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_pt_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)orig_io->driver_ctx;

	/* We setup this value in the submission routine, just showing here that it is
	 * passed back to us.
	 */
	if (io_ctx->test != 0x5a) {
		SPDK_ERRLOG("Error, original IO device_ctx is wrong! 0x%x\n",
			    io_ctx->test);
	}

	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_complete_base_io_status(orig_io, bdev_io);

	spdk_bdev_free_io(bdev_io);
}

static void
_pt_complete_zcopy_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)orig_io->driver_ctx;

	/* We setup this value in the submission routine, just showing here that it is
	 * passed back to us.
	 */
	if (io_ctx->test != 0x5a) {
		SPDK_ERRLOG("Error, original IO device_ctx is wrong! 0x%x\n",
			    io_ctx->test);
	}

	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_set_buf(orig_io, bdev_io->u.bdev.iovs[0].iov_base, bdev_io->u.bdev.iovs[0].iov_len);
	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
vbdev_wtcache_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)bdev_io->driver_ctx;

	vbdev_wtcache_submit_request(io_ctx->ch, bdev_io);
}

static void
vbdev_wtcache_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)bdev_io->driver_ctx;
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	int rc;

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_wtcache_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	/* Queue the IO using the channel of the base device. */
	rc = spdk_bdev_queue_io_wait(bdev_io->bdev, pt_ch->base_ch, &io_ctx->bdev_io_wait);
	if (rc != 0) {
		SPDK_ERRLOG("Queue io failed in vbdev_wtcache_queue_io, rc=%d.\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

/* ================================================================== */
/* [wtcache] writeback 스레드                                          */
/* 아래 함수들은 전부 pt_node->wb_thread 위에서만 실행된다.            */
/* ================================================================== */

static void wtcache_wb_run(struct vbdev_wtcache *pt_node);
static void wtcache_wb_check_flushes(struct vbdev_wtcache *pt_node);
static void wtcache_wb_check_drain(struct vbdev_wtcache *pt_node);

/* bdev_io는 자기가 제출된 스레드에서 완료해야 하므로 메시지로 넘겨서 완료 */
static void
wtcache_complete_msg(void *ctx)
{
	struct wtcache_bdev_io *io_ctx = ctx;

	spdk_bdev_io_complete(io_ctx->bdev_io, io_ctx->failed ? SPDK_BDEV_IO_STATUS_FAILED :
			      SPDK_BDEV_IO_STATUS_SUCCESS);
}

static void
wtcache_complete_on_io_thread(struct wtcache_bdev_io *io_ctx)
{
	spdk_thread_send_msg(spdk_bdev_io_get_thread(io_ctx->bdev_io), wtcache_complete_msg, io_ctx);
}

/* writeback 한 건이 base에서 끝남 */
static void
wtcache_wb_write_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wtc_wb_io *io = cb_arg;
	struct vbdev_wtcache *pt_node = io->priv;

	spdk_bdev_free_io(bdev_io);
	wtc_wb_done(pt_node->cache, io, success);
	pt_node->wb_inflight--;

	wtcache_wb_run(pt_node);		/* 다음 것 내보내기 */
	wtcache_wb_check_flushes(pt_node);	/* 기다리던 Flush가 끝났는지 */
	wtcache_wb_check_drain(pt_node);	/* 삭제 중이면 다 내려갔는지 */
}

/* dirty extent를 꺼내 base에 쓴다 */
static void
wtcache_wb_run(struct vbdev_wtcache *pt_node)
{
	struct wtc_wb_io *ios[16];
	int n, i, want, rc;

	if (pt_node->wb_ch == NULL) {
		return;
	}
	while (pt_node->wb_inflight < WTCACHE_WB_MAX_INFLIGHT) {
		want = WTCACHE_WB_MAX_INFLIGHT - pt_node->wb_inflight;
		n = wtc_wb_next(pt_node->cache, ios, want < 16 ? want : 16);
		if (n == 0) {
			return;
		}
		for (i = 0; i < n; i++) {
			ios[i]->priv = pt_node;
			rc = spdk_bdev_writev_blocks(pt_node->base_desc, pt_node->wb_ch,
						     ios[i]->iovs, ios[i]->iovcnt,
						     ios[i]->lba, ios[i]->nblocks,
						     wtcache_wb_write_done, ios[i]);
			if (rc != 0) {
				/* bdev_io 부족 등: 실패로 세지 않고 되돌려 놓고 다음 poller 때 재시도 */
				for (; i < n; i++) {
					wtc_wb_requeue(pt_node->cache, ios[i]);
				}
				return;
			}
			pt_node->wb_inflight++;
		}
	}
}

static void
wtcache_base_flush_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wtcache_bdev_io *io_ctx = cb_arg;

	spdk_bdev_free_io(bdev_io);
	if (!success) {
		io_ctx->failed = true;
	}
	wtcache_complete_on_io_thread(io_ctx);
}

/* Flush 표시 이전 쓰기가 다 내려갔으면 base flush 후 완료 */
static void
wtcache_wb_check_flushes(struct vbdev_wtcache *pt_node)
{
	struct wtcache_bdev_io *io_ctx, *tmp;
	int rc;

	if (pt_node->wb_ch == NULL) {
		return;
	}
	TAILQ_FOREACH_SAFE(io_ctx, &pt_node->flushes, link, tmp) {
		if (!wtc_flush_reached(pt_node->cache, io_ctx->flush_mark)) {
			continue;
		}
		TAILQ_REMOVE(&pt_node->flushes, io_ctx, link);
		if (wtc_take_error(pt_node->cache)) {
			io_ctx->failed = true;	/* writeback을 포기한 데이터가 있었음 */
		}
		/* SSD 내부 휘발성 캐시까지 비우기 */
		rc = spdk_bdev_flush_blocks(pt_node->base_desc, pt_node->wb_ch, 0,
					    spdk_bdev_get_num_blocks(pt_node->base_bdev),
					    wtcache_base_flush_done, io_ctx);
		if (rc == -ENOMEM) {
			TAILQ_INSERT_HEAD(&pt_node->flushes, io_ctx, link);	/* 다음에 재시도 */
			return;
		} else if (rc != 0) {
			io_ctx->failed = true;
			wtcache_complete_on_io_thread(io_ctx);
		}
	}
}

/* 삭제(destruct) 중이고 다 내려갔으면 writeback 스레드를 정리 */
static void
wtcache_wb_check_drain(struct vbdev_wtcache *pt_node)
{
	if (!pt_node->draining || pt_node->wb_inflight > 0 || wtc_pending(pt_node->cache) > 0) {
		return;
	}
	pt_node->draining = false;
	spdk_poller_unregister(&pt_node->wb_poller);
	if (pt_node->wb_ch != NULL) {
		spdk_put_io_channel(pt_node->wb_ch);
		pt_node->wb_ch = NULL;
	}
	pt_node->wb_thread = NULL;
	spdk_thread_exit(spdk_get_thread());
	spdk_thread_send_msg(pt_node->thread, wtcache_destruct_finish, pt_node);
}

static void
wtcache_wb_drain_start(void *ctx)
{
	struct vbdev_wtcache *pt_node = ctx;

	SPDK_NOTICELOG("%s: writing back %lu dirty extents before removal\n",
		       pt_node->pt_bdev.name, wtc_pending(pt_node->cache));
	pt_node->draining = true;
	if (pt_node->wb_ch == NULL) {
		/* base 채널을 못 얻은 상태: 내려쓸 방법이 없으니 그냥 정리 */
		while (wtc_pending(pt_node->cache) > 0) {
			struct wtc_wb_io *io;

			if (wtc_wb_next(pt_node->cache, &io, 1) == 0) {
				break;
			}
			wtc_wb_done(pt_node->cache, io, false);
		}
	}
	wtcache_wb_run(pt_node);
	wtcache_wb_check_drain(pt_node);
}

static int
wtcache_wb_poller(void *ctx)
{
	struct vbdev_wtcache *pt_node = ctx;
	int before = pt_node->wb_inflight;
	int rc;

	wtcache_wb_run(pt_node);
	wtcache_wb_check_flushes(pt_node);
	rc = pt_node->wb_inflight != before ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
	wtcache_wb_check_drain(pt_node);
	return rc;
}

/* writeback 스레드의 첫 메시지: base 채널과 poller 준비 */
static void
wtcache_wb_start(void *ctx)
{
	struct vbdev_wtcache *pt_node = ctx;

	pt_node->wb_ch = spdk_bdev_get_io_channel(pt_node->base_desc);
	if (pt_node->wb_ch == NULL) {
		SPDK_ERRLOG("%s: failed to get base channel for writeback\n", pt_node->pt_bdev.name);
		return;
	}
	pt_node->wb_poller = SPDK_POLLER_REGISTER(wtcache_wb_poller, pt_node, WTCACHE_WB_POLL_US);
}

/* I/O 스레드 -> writeback 스레드: Flush 등록 */
static void
wtcache_wb_add_flush(void *ctx)
{
	struct wtcache_bdev_io *io_ctx = ctx;
	struct vbdev_wtcache *pt_node = io_ctx->node;

	TAILQ_INSERT_TAIL(&pt_node->flushes, io_ctx, link);
	wtcache_wb_run(pt_node);
	wtcache_wb_check_flushes(pt_node);
}

/* I/O 스레드 -> writeback 스레드: "dirty 생겼으니 내려써" */
static void
wtcache_wb_kick_msg(void *ctx)
{
	struct vbdev_wtcache *pt_node = ctx;

	__atomic_store_n(&pt_node->kick_pending, false, __ATOMIC_RELEASE);
	wtcache_wb_run(pt_node);
}

/* 아무 스레드에서나 호출 가능. 이미 깨우는 중이면 메시지를 또 보내지 않는다 */
static void
wtcache_kick_writeback(struct vbdev_wtcache *pt_node)
{
	struct spdk_thread *wb_thread = pt_node->wb_thread;

	if (wb_thread == NULL) {
		return;
	}
	if (!__atomic_exchange_n(&pt_node->kick_pending, true, __ATOMIC_ACQ_REL)) {
		if (spdk_thread_send_msg(wb_thread, wtcache_wb_kick_msg, pt_node) != 0) {
			__atomic_store_n(&pt_node->kick_pending, false, __ATOMIC_RELEASE);
		}
	}
}

/* ================================================================== */
/* [wtcache] 읽기 보조: 부분 히트일 때 캐시에 없는 구간만 base에서 읽기 */
/* ================================================================== */

struct wtcache_read_run {
	struct spdk_bdev_io	*orig_io;
	struct iovec		iov[];
};

/* src iov들 중 [off, off+len) 바이트 구간을 가리키는 iov 배열을 dst에 만든다 */
static int
wtcache_iov_slice(const struct iovec *src, int srccnt, size_t off, size_t len, struct iovec *dst)
{
	int i = 0, n = 0;

	while (i < srccnt && off >= src[i].iov_len) {
		off -= src[i].iov_len;
		i++;
	}
	while (len > 0 && i < srccnt) {
		size_t l = src[i].iov_len - off;

		if (l > len) {
			l = len;
		}
		dst[n].iov_base = (char *)src[i].iov_base + off;
		dst[n].iov_len = l;
		n++;
		len -= l;
		off = 0;
		i++;
	}
	return n;
}

static void
wtcache_read_run_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wtcache_read_run *run = cb_arg;
	struct spdk_bdev_io *orig_io = run->orig_io;
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)orig_io->driver_ctx;

	spdk_bdev_free_io(bdev_io);
	free(run);
	if (!success) {
		io_ctx->failed = true;
	}
	if (--io_ctx->read_remaining == 0) {
		spdk_bdev_io_complete(orig_io, io_ctx->failed ? SPDK_BDEV_IO_STATUS_FAILED :
				      SPDK_BDEV_IO_STATUS_SUCCESS);
	}
}

/* runs[] 구간들을 base에서 읽어 bdev_io의 iov 해당 위치에 채운다 */
static void
wtcache_read_runs(struct vbdev_wtcache *pt_node, struct pt_io_channel *pt_ch,
		  struct spdk_bdev_io *bdev_io, struct wtc_run *runs, int nr)
{
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)bdev_io->driver_ctx;
	uint32_t bl = pt_node->pt_bdev.blocklen;
	uint64_t lba = bdev_io->u.bdev.offset_blocks;
	int k, rc;

	io_ctx->failed = false;
	io_ctx->read_remaining = nr;
	for (k = 0; k < nr; k++) {
		struct wtcache_read_run *run;
		int cnt;

		run = malloc(sizeof(*run) + sizeof(struct iovec) * bdev_io->u.bdev.iovcnt);
		rc = -ENOMEM;
		if (run != NULL) {
			run->orig_io = bdev_io;
			cnt = wtcache_iov_slice(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						(runs[k].lba - lba) * bl, runs[k].nblocks * bl, run->iov);
			rc = spdk_bdev_readv_blocks(pt_node->base_desc, pt_ch->base_ch, run->iov, cnt,
						    runs[k].lba, runs[k].nblocks,
						    wtcache_read_run_done, run);
		}
		if (rc != 0) {
			free(run);
			io_ctx->failed = true;
			if (--io_ctx->read_remaining == 0) {
				spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
		}
	}
}

static void
pt_init_ext_io_opts(struct spdk_bdev_io *bdev_io, struct spdk_bdev_ext_io_opts *opts)
{
	memset(opts, 0, sizeof(*opts));
	opts->size = sizeof(*opts);
	opts->memory_domain = bdev_io->u.bdev.memory_domain;
	opts->memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	opts->metadata = bdev_io->u.bdev.md_buf;
	opts->dif_check_flags_exclude_mask = ~bdev_io->u.bdev.dif_check_flags;
}

/* Callback for getting a buf from the bdev pool in the event that the caller passed
 * in NULL, we need to own the buffer so it doesn't get freed by another vbdev module
 * beneath us before we're done with it. That won't happen in this example but it could
 * if this example were used as a template for something more complex.
 */
static void
pt_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_wtcache *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_wtcache,
					 pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)bdev_io->driver_ctx;
	struct spdk_bdev_ext_io_opts io_opts;
	struct wtc_run runs_stack[8], *runs = runs_stack;
	uint64_t lba = bdev_io->u.bdev.offset_blocks;
	uint64_t nblocks = bdev_io->u.bdev.num_blocks;
	int max_runs = WTC_MAX_RUNS(nblocks);
	int nr;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	/* [wtcache] 캐시에 있는 블록은 여기서 바로 iov로 복사되고,
	 * 없는 구간만 runs[]로 돌아온다. */
	if (max_runs > (int)SPDK_COUNTOF(runs_stack)) {
		runs = malloc(sizeof(*runs) * max_runs);
		if (runs == NULL) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
			return;
		}
	}
	nr = wtc_read(pt_node->cache, lba, nblocks, bdev_io->u.bdev.iovs,
		      bdev_io->u.bdev.iovcnt, runs, max_runs);

	if (nr == 0) {
		/* 전부 캐시 히트 */
		if (runs != runs_stack) {
			free(runs);
		}
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return;
	}
	if (nr < 0 || nr > 1 || runs[0].lba != lba || runs[0].nblocks != nblocks) {
		/* 일부만 히트: 빠진 구간만 base에서 읽는다 */
		if (nr > 0) {
			wtcache_read_runs(pt_node, pt_ch, bdev_io, runs, nr);
		} else {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		if (runs != runs_stack) {
			free(runs);
		}
		return;
	}
	if (runs != runs_stack) {
		free(runs);
	}

	/* 전부 미스: 원래 passthru처럼 통째로 base에서 읽는다 */
	pt_init_ext_io_opts(bdev_io, &io_opts);
	rc = spdk_bdev_readv_blocks_ext(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
					bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					bdev_io->u.bdev.num_blocks, _pt_complete_io,
					bdev_io, &io_opts);
	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for wtcache.\n");
			io_ctx->ch = ch;
			vbdev_wtcache_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

/* Called when someone above submits IO to this pt vbdev. We're simply passing it on here
 * via SPDK IO calls which in turn allocate another bdev IO and call our cpl callback provided
 * below along with the original bdev_io so that we can complete it once this IO completes.
 */
static void
vbdev_wtcache_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_wtcache *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_wtcache, pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct wtcache_bdev_io *io_ctx = (struct wtcache_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	/* Setup a per IO context value; we don't do anything with it in the vbdev other
	 * than confirm we get the same thing back in the completion callback just to
	 * demonstrate.
	 */
	io_ctx->test = 0x5a;
	io_ctx->bdev_io = bdev_io;	/* [wtcache] */
	io_ctx->node = pt_node;		/* [wtcache] */

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, pt_read_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		/* [wtcache] 캐시에 넣고 바로 응답. 장치로는 writeback 스레드가 내린다 */
		rc = wtc_write(pt_node->cache, bdev_io->u.bdev.offset_blocks,
			       bdev_io->u.bdev.num_blocks,
			       bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt);
		if (rc == 0 && (bdev_io->u.bdev.nvme_cdw12.raw & SPDK_NVME_IO_FLAGS_FORCE_UNIT_ACCESS)) {
			io_ctx->failed = false;
			io_ctx->flush_mark = wtc_flush_mark(pt_node->cache);
			if (pt_node->wb_thread == NULL ||
			    spdk_thread_send_msg(pt_node->wb_thread, wtcache_wb_add_flush, io_ctx) != 0) {
				spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
			wtcache_kick_writeback(pt_node);
			return;
		}
		if (rc == 0) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		} else if (rc == -ENOMEM) {
			/* 캐시가 dirty로 꽉 참: bdev 계층이 나중에 다시 제출해 준다 */
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
		} else {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		wtcache_kick_writeback(pt_node);
		return;		/* break 아님: 아래 공통 에러 처리를 타면 안 된다 */
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		rc = spdk_bdev_write_zeroes_blocks(pt_node->base_desc, pt_ch->base_ch,
						   bdev_io->u.bdev.offset_blocks,
						   bdev_io->u.bdev.num_blocks,
						   _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(pt_node->base_desc, pt_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		/* [wtcache] 지금까지의 쓰기를 표시해 writeback 스레드에 넘긴다.
		 * 그 쓰기들이 전부 내려가면 writeback 스레드가 base flush 후 완료시킨다. */
		io_ctx->failed = false;
		io_ctx->flush_mark = wtc_flush_mark(pt_node->cache);
		if (pt_node->wb_thread == NULL ||
		    spdk_thread_send_msg(pt_node->wb_thread, wtcache_wb_add_flush, io_ctx) != 0) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		return;
	case SPDK_BDEV_IO_TYPE_RESET:
		rc = spdk_bdev_reset(pt_node->base_desc, pt_ch->base_ch,
				     _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		rc = spdk_bdev_zcopy_start(pt_node->base_desc, pt_ch->base_ch, NULL, 0,
					   bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.num_blocks, bdev_io->u.bdev.zcopy.populate,
					   _pt_complete_zcopy_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = spdk_bdev_abort(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.abort.bio_to_abort,
				     _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_COMPARE:
		rc = spdk_bdev_comparev_blocks(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
					       bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					       bdev_io->u.bdev.num_blocks,
					       _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_COPY:
		rc = spdk_bdev_copy_blocks(pt_node->base_desc, pt_ch->base_ch,
					   bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.copy.src_offset_blocks,
					   bdev_io->u.bdev.num_blocks,
					   _pt_complete_io, bdev_io);
		break;
	default:
		SPDK_ERRLOG("wtcache: unknown I/O type %d\n", bdev_io->type);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}
	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for wtcache.\n");
			io_ctx->ch = ch;
			vbdev_wtcache_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

/* We'll just call the base bdev and let it answer however if we were more
 * restrictive for some reason (or less) we could get the response back
 * and modify according to our purposes.
 */
static bool
vbdev_wtcache_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_wtcache *pt_node = (struct vbdev_wtcache *)ctx;

	/* [wtcache] 캐시가 처리하는 것만 허용. UNMAP/WRITE_ZEROES 등은 캐시와 순서가
	 * 꼬일 수 있어 막는다 (nvmf가 이를 보고 호스트에 미지원으로 알림) */
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_FLUSH:
		return true;
	case SPDK_BDEV_IO_TYPE_RESET:
		return spdk_bdev_io_type_supported(pt_node->base_bdev, io_type);
	default:
		return false;
	}
}

/* We supplied this as an entry point for upper layers who want to communicate to this
 * bdev.  This is how they get a channel. We are passed the same context we provided when
 * we created our PT vbdev in examine() which, for this bdev, is the address of one of
 * our context nodes. From here we'll ask the SPDK channel code to fill out our channel
 * struct and we'll keep it in our PT node.
 */
static struct spdk_io_channel *
vbdev_wtcache_get_io_channel(void *ctx)
{
	struct vbdev_wtcache *pt_node = (struct vbdev_wtcache *)ctx;
	struct spdk_io_channel *pt_ch = NULL;

	/* The IO channel code will allocate a channel for us which consists of
	 * the SPDK channel structure plus the size of our pt_io_channel struct
	 * that we passed in when we registered our IO device. It will then call
	 * our channel create callback to populate any elements that we need to
	 * update.
	 */
	pt_ch = spdk_get_io_channel(pt_node);

	return pt_ch;
}

/* This is the output for bdev_get_bdevs() for this vbdev */
static int
vbdev_wtcache_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_wtcache *pt_node = (struct vbdev_wtcache *)ctx;

	spdk_json_write_name(w, "wtcache");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
	spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
	{
		/* [wtcache] rpc.py bdev_get_bdevs -b <name> 에서 보이는 통계 */
		struct wtc_stats st;

		wtc_get_stats(pt_node->cache, &st);
		spdk_json_write_named_object_begin(w, "cache");
		spdk_json_write_named_uint64(w, "total_bytes", st.total_bytes);
		spdk_json_write_named_uint64(w, "used_bytes", st.used_bytes);
		spdk_json_write_named_uint64(w, "pending_extents", st.pending);
		spdk_json_write_named_uint64(w, "read_blocks", st.read_blocks);
		spdk_json_write_named_uint64(w, "read_hit_blocks", st.read_hit_blocks);
		spdk_json_write_named_uint64(w, "write_blocks", st.write_blocks);
		spdk_json_write_named_uint64(w, "write_nomem", st.write_nomem);
		spdk_json_write_named_uint64(w, "evictions", st.evictions);
		spdk_json_write_named_uint64(w, "writebacks", st.writebacks);
		spdk_json_write_named_uint64(w, "wb_errors", st.wb_errors);
		spdk_json_write_named_uint64(w, "wb_dropped", st.wb_dropped);
		spdk_json_write_object_end(w);
	}
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_wtcache_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_wtcache *pt_node;

	TAILQ_FOREACH(pt_node, &g_pt_nodes, link) {
		const struct spdk_uuid *uuid = spdk_bdev_get_uuid(&pt_node->pt_bdev);

		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_wtcache_create");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
		spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
		if (!spdk_uuid_is_null(uuid)) {
			spdk_json_write_named_uuid(w, "uuid", uuid);
		}
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

/* We provide this callback for the SPDK channel code to create a channel using
 * the channel struct we provided in our module get_io_channel() entry point. Here
 * we get and save off an underlying base channel of the device below us so that
 * we can communicate with the base bdev on a per channel basis.  If we needed
 * our own poller for this vbdev, we'd register it here.
 */
static int
pt_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;
	struct vbdev_wtcache *pt_node = io_device;

	pt_ch->base_ch = spdk_bdev_get_io_channel(pt_node->base_desc);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregister it here.
 */
static void
pt_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;

	spdk_put_io_channel(pt_ch->base_ch);
}

/* Create the wtcache association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_wtcache_insert_name(const char *bdev_name, const char *vbdev_name,
			   const struct spdk_uuid *uuid)
{
	struct bdev_names *name;

	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(vbdev_name, name->vbdev_name) == 0) {
			SPDK_ERRLOG("wtcache bdev %s already exists\n", vbdev_name);
			return -EEXIST;
		}
	}

	name = calloc(1, sizeof(struct bdev_names));
	if (!name) {
		SPDK_ERRLOG("could not allocate bdev_names\n");
		return -ENOMEM;
	}

	name->bdev_name = strdup(bdev_name);
	if (!name->bdev_name) {
		SPDK_ERRLOG("could not allocate name->bdev_name\n");
		free(name);
		return -ENOMEM;
	}

	name->vbdev_name = strdup(vbdev_name);
	if (!name->vbdev_name) {
		SPDK_ERRLOG("could not allocate name->vbdev_name\n");
		free(name->bdev_name);
		free(name);
		return -ENOMEM;
	}

	spdk_uuid_copy(&name->uuid, uuid);
	TAILQ_INSERT_TAIL(&g_bdev_names, name, link);

	return 0;
}

/* On init, just perform bdev module specific initialization. */
static int
vbdev_wtcache_init(void)
{
	return 0;
}

/* Called when the entire module is being torn down. */
static void
vbdev_wtcache_finish(void)
{
	struct bdev_names *name;

	while ((name = TAILQ_FIRST(&g_bdev_names))) {
		TAILQ_REMOVE(&g_bdev_names, name, link);
		free(name->bdev_name);
		free(name->vbdev_name);
		free(name);
	}
}

/* During init we'll be asked how much memory we'd like passed to us
 * in bev_io structures as context. Here's where we specify how
 * much context we want per IO.
 */
static int
vbdev_wtcache_get_ctx_size(void)
{
	return sizeof(struct wtcache_bdev_io);
}

/* Where vbdev_wtcache_config_json() is used to generate per module JSON config data, this
 * function is called to output any per bdev specific methods. For the PT module, there are
 * none.
 */
static void
vbdev_wtcache_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	/* No config per bdev needed */
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_wtcache_fn_table = {
	.destruct		= vbdev_wtcache_destruct,
	.submit_request		= vbdev_wtcache_submit_request,
	.io_type_supported	= vbdev_wtcache_io_type_supported,
	.get_io_channel		= vbdev_wtcache_get_io_channel,
	.dump_info_json		= vbdev_wtcache_dump_info_json,
	.write_config_json	= vbdev_wtcache_write_config_json,
	/* [wtcache] get_memory_domains 제거: 데이터를 CPU로 복사하므로 일반 메모리만 받는다 */
};

static void
vbdev_wtcache_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_wtcache *pt_node, *tmp;

	TAILQ_FOREACH_SAFE(pt_node, &g_pt_nodes, link, tmp) {
		if (bdev_find == pt_node->base_bdev) {
			spdk_bdev_unregister(&pt_node->pt_bdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_wtcache_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
				  void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_wtcache_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/* Create and register the wtcache vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_wtcache_register(const char *bdev_name)
{
	struct bdev_names *name;
	struct vbdev_wtcache *pt_node;
	struct spdk_bdev *bdev;
	struct spdk_uuid ns_uuid;
	int rc = 0;

	spdk_uuid_parse(&ns_uuid, BDEV_WTCACHE_NAMESPACE_UUID);

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the pt_node & bdev accordingly.
	 */
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->bdev_name, bdev_name) != 0) {
			continue;
		}

		SPDK_NOTICELOG("Match on %s\n", bdev_name);
		pt_node = calloc(1, sizeof(struct vbdev_wtcache));
		if (!pt_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_node\n");
			break;
		}

		pt_node->pt_bdev.name = strdup(name->vbdev_name);
		if (!pt_node->pt_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_bdev name\n");
			free(pt_node);
			break;
		}
		pt_node->pt_bdev.product_name = "wtcache";

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_wtcache_base_bdev_event_cb,
					NULL, &pt_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("base bdev opened\n");

		bdev = spdk_bdev_desc_get_bdev(pt_node->base_desc);
		pt_node->base_bdev = bdev;

		/* [wtcache] 메타데이터/DIF가 있거나 블록이 4KiB보다 크면 지원 안 함 */
		if (bdev->md_len != 0 || bdev->blocklen > WTC_CHUNK_SIZE ||
		    WTC_CHUNK_SIZE % bdev->blocklen != 0) {
			SPDK_ERRLOG("base bdev %s: unsupported format (blocklen %u, md_len %u)\n",
				    bdev_name, bdev->blocklen, bdev->md_len);
			spdk_bdev_close(pt_node->base_desc);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			rc = -EINVAL;
			break;
		}

		if (!spdk_uuid_is_null(&name->uuid)) {
			/* Use the configured UUID */
			spdk_uuid_copy(&pt_node->pt_bdev.uuid, &name->uuid);
		} else {
			/* Generate UUID based on namespace UUID + base bdev UUID. */
			rc = spdk_uuid_generate_sha1(&pt_node->pt_bdev.uuid, &ns_uuid,
						     (const char *)&pt_node->base_bdev->uuid, sizeof(struct spdk_uuid));
			if (rc) {
				SPDK_ERRLOG("Unable to generate new UUID for wtcache bdev\n");
				spdk_bdev_close(pt_node->base_desc);
				free(pt_node->pt_bdev.name);
				free(pt_node);
				break;
			}
		}

		/* Copy some properties from the underlying base bdev. */
		/* [wtcache] 항상 휘발성 쓰기 캐시가 있다고 알린다 -> nvmf가 VWC=1 광고 -> 호스트가 Flush를 보냄 */
		pt_node->pt_bdev.write_cache = 1;
		pt_node->pt_bdev.required_alignment = bdev->required_alignment;
		pt_node->pt_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		pt_node->pt_bdev.blocklen = bdev->blocklen;
		pt_node->pt_bdev.blockcnt = bdev->blockcnt;

		pt_node->pt_bdev.md_interleave = bdev->md_interleave;
		pt_node->pt_bdev.md_len = bdev->md_len;
		pt_node->pt_bdev.dif_type = bdev->dif_type;
		pt_node->pt_bdev.dif_is_head_of_md = bdev->dif_is_head_of_md;
		pt_node->pt_bdev.dif_check_flags = bdev->dif_check_flags;
		pt_node->pt_bdev.dif_pi_format = bdev->dif_pi_format;
		pt_node->pt_bdev.dif_storage_tag_size = bdev->dif_storage_tag_size;

		pt_node->pt_bdev.numa = bdev->numa;

		/* This is the context that is passed to us when the bdev
		 * layer calls in so we'll save our pt_bdev node here.
		 */
		pt_node->pt_bdev.ctxt = pt_node;
		pt_node->pt_bdev.fn_table = &vbdev_wtcache_fn_table;
		pt_node->pt_bdev.module = &wtcache_if;

		/* [wtcache] 캐시 메모리 (hugepage, NVMe로 바로 DMA 가능) */
		{
			struct wtc_opts opts = {
				.buf_size = (uint64_t)WTCACHE_CACHE_MB << 20,
				.blocklen = bdev->blocklen,
			};

			pt_node->cache_mem = spdk_dma_zmalloc(opts.buf_size, WTC_CHUNK_SIZE, NULL);
			opts.buf = pt_node->cache_mem;
			pt_node->cache = pt_node->cache_mem ? wtc_create(&opts) : NULL;
			if (pt_node->cache == NULL) {
				SPDK_ERRLOG("could not allocate %u MiB cache (check hugepages)\n",
					    WTCACHE_CACHE_MB);
				spdk_dma_free(pt_node->cache_mem);
				spdk_bdev_close(pt_node->base_desc);
				free(pt_node->pt_bdev.name);
				free(pt_node);
				rc = -ENOMEM;
				break;
			}
			TAILQ_INIT(&pt_node->flushes);
		}

		TAILQ_INSERT_TAIL(&g_pt_nodes, pt_node, link);

		spdk_io_device_register(pt_node, pt_bdev_ch_create_cb, pt_bdev_ch_destroy_cb,
					sizeof(struct pt_io_channel),
					name->vbdev_name);
		SPDK_NOTICELOG("io_device created at: %p\n", pt_node);

		/* Save the thread where the base device is opened */
		pt_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(bdev, pt_node->base_desc, pt_node->pt_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			wtc_destroy(pt_node->cache);		/* [wtcache] */
			spdk_dma_free(pt_node->cache_mem);	/* [wtcache] */
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("bdev claimed\n");

		rc = spdk_bdev_register(&pt_node->pt_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register pt_bdev\n");
			spdk_bdev_module_release_bdev(&pt_node->pt_bdev);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			wtc_destroy(pt_node->cache);		/* [wtcache] */
			spdk_dma_free(pt_node->cache_mem);	/* [wtcache] */
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("pt_bdev registered\n");

		/* [wtcache] writeback 전용 스레드 생성. 첫 메시지(wtcache_wb_start)를 보낸 뒤에
		 * wb_thread를 채워야, I/O 쪽의 깨우기 메시지가 항상 그보다 뒤에 도착한다. */
		{
			struct spdk_thread *wb_thread = spdk_thread_create(name->vbdev_name, NULL);

			if (wb_thread == NULL) {
				SPDK_ERRLOG("could not create writeback thread; writes will not reach %s\n",
					    bdev_name);
			} else {
				spdk_thread_send_msg(wb_thread, wtcache_wb_start, pt_node);
				pt_node->wb_thread = wb_thread;
			}
		}
		SPDK_NOTICELOG("created pt_bdev for: %s (cache %u MiB)\n", name->vbdev_name,
			       WTCACHE_CACHE_MB);
	}

	return rc;
}

/* Create the wtcache disk from the given bdev and vbdev name. */
int
bdev_wtcache_create_disk(const char *bdev_name, const char *vbdev_name,
			  const struct spdk_uuid *uuid)
{
	int rc;

	/* Insert the bdev name into our global name list even if it doesn't exist yet,
	 * it may show up soon...
	 */
	rc = vbdev_wtcache_insert_name(bdev_name, vbdev_name, uuid);
	if (rc) {
		return rc;
	}

	rc = vbdev_wtcache_register(bdev_name);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vbdev creation deferred pending base bdev arrival\n");
		rc = 0;
	}

	return rc;
}

void
bdev_wtcache_delete_disk(const char *bdev_name, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct bdev_names *name;
	int rc;

	/* Some cleanup happens in the destruct callback. */
	rc = spdk_bdev_unregister_by_name(bdev_name, &wtcache_if, cb_fn, cb_arg);
	if (rc == 0) {
		/* Remove the association (vbdev, bdev) from g_bdev_names. This is required so that the
		 * vbdev does not get re-created if the same bdev is constructed at some other time,
		 * unless the underlying bdev was hot-removed.
		 */
		TAILQ_FOREACH(name, &g_bdev_names, link) {
			if (strcmp(name->vbdev_name, bdev_name) == 0) {
				TAILQ_REMOVE(&g_bdev_names, name, link);
				free(name->bdev_name);
				free(name->vbdev_name);
				free(name);
				break;
			}
		}
	} else {
		cb_fn(cb_arg, rc);
	}
}

/* Because we specified this function in our pt bdev function table when we
 * registered our pt bdev, we'll get this call anytime a new bdev shows up.
 * Here we need to decide if we care about it and if so what to do. We
 * parsed the config file at init so we check the new bdev against the list
 * we built up at that time and if the user configured us to attach to this
 * bdev, here's where we do it.
 */
static void
vbdev_wtcache_examine(struct spdk_bdev *bdev)
{
	vbdev_wtcache_register(bdev->name);

	spdk_bdev_module_examine_done(&wtcache_if);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_wtcache)
