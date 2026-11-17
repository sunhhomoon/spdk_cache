/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_VBDEV_WTCACHE_H
#define SPDK_VBDEV_WTCACHE_H

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

/**
 * Create new pass through bdev.
 *
 * \param bdev_name Bdev on which pass through vbdev will be created.
 * \param vbdev_name Name of the pass through bdev.
 * \param uuid Optional UUID to assign to the pass through bdev.
 * \return 0 on success, other on failure.
 */
int bdev_wtcache_create_disk(const char *bdev_name, const char *vbdev_name,
			      const struct spdk_uuid *uuid);

/**
 * Delete wtcache bdev.
 *
 * \param bdev_name Name of the pass through bdev.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void bdev_wtcache_delete_disk(const char *bdev_name, spdk_bdev_unregister_cb cb_fn,
			       void *cb_arg);

#endif /* SPDK_VBDEV_WTCACHE_H */
