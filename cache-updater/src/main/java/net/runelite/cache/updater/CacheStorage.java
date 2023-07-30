/*
 * Copyright (c) 2017, Adam <Adam@sigterm.info>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.runelite.cache.updater;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.runelite.cache.fs.Archive;
import net.runelite.cache.fs.Container;
import net.runelite.cache.fs.Index;
import net.runelite.cache.fs.Storage;
import net.runelite.cache.fs.Store;
import net.runelite.cache.index.ArchiveData;
import net.runelite.cache.index.IndexData;
import net.runelite.cache.updater.beans.ArchiveEntry;
import net.runelite.cache.updater.beans.CacheEntry;

public class CacheStorage implements Storage
{
	private CacheEntry cacheEntry;
	private final CacheDAO cacheDao;
	private final Map<Long, Integer> dataIds = new HashMap<>();

	public CacheStorage(CacheEntry cacheEntry, CacheDAO cacheDao)
	{
		this.cacheEntry = cacheEntry;
		this.cacheDao = cacheDao;
	}

	public CacheEntry getCacheEntry()
	{
		return cacheEntry;
	}

	public void setCacheEntry(CacheEntry cacheEntry)
	{
		this.cacheEntry = cacheEntry;
	}

	@Override
	public void init(Store store)
	{
	}

	@Override
	public void close()
	{
	}

	@Override
	public void load(Store store) throws IOException
	{
		List<ArchiveEntry> indexes = cacheDao.findIndexesForCache(cacheEntry);
		for (ArchiveEntry indexEntry : indexes)
		{
			Index index = store.addIndex(indexEntry.getArchiveId());
			index.setCrc(indexEntry.getCrc());
			index.setRevision(indexEntry.getRevision());

			// index data includes file data too, which isn't stored otherwise. so we need to load this
			// instead of reading from the archive tables.
			byte[] indexData = cacheDao.readData(indexEntry.getDataId());
			if (indexData == null)
			{
				throw new IOException("missing index data");
			}

			Container res = Container.decompress(indexData, null);
			byte[] data = res.data;

			IndexData id = new IndexData();
			id.load(data);

			index.setProtocol(id.getProtocol());
			index.setRevision(id.getRevision());
			index.setNamed(id.isNamed());
			index.setCrc(res.crc);
			index.setCompression(res.compression);

			for (ArchiveData ad : id.getArchives())
			{
				Archive archive = index.addArchive(ad.getId());
				archive.setNameHash(ad.getNameHash());
				archive.setCrc(ad.getCrc());
				archive.setRevision(ad.getRevision());
				archive.setFileData(ad.getFiles());

				assert ad.getFiles().length > 0;
			}

			assert res.revision == -1;
		}

		// load dataIds here?
	}

	@Override
	public void save(Store store) throws IOException
	{
		for (Index index : store.getIndexes())
		{
			saveIndex(index);

			for (Archive archive : index.getArchives())
			{
				int id = cacheDao.findArchive(index.getId(), archive.getArchiveId(), archive.getCrc(), archive.getNameHash(), archive.getRevision());
				if (id == -1)
				{
					Integer dataId = dataIds.get((long) index.getId() << 32 | archive.getArchiveId());
					if (dataId == null)
					{
						throw new RuntimeException("no data for " + index.getId() + "/" + archive.getArchiveId());
					}
					id = cacheDao.insertArchive(index.getId(), archive.getArchiveId(), archive.getCrc(), archive.getNameHash(), archive.getRevision(), dataId);
				}
				cacheDao.linkArchive(cacheEntry.getId(), id);
			}
		}
	}

	private void saveIndex(Index index) throws IOException
	{
		IndexData indexData = index.toIndexData();
		byte[] data = indexData.writeIndexData();

		Container container = new Container(index.getCompression(), -1); // index data revision is always -1
		container.compress(data, null);
		byte[] compressedData = container.data;

		int id = cacheDao.findArchive(255, index.getId(), container.crc, 0, index.getRevision());
		if (id == -1)
		{
			int dataId = cacheDao.insertData(compressedData);
			id = cacheDao.insertArchive(255, index.getId(), container.crc, 0, index.getRevision(), dataId);
		}

		cacheDao.linkArchive(cacheEntry.getId(), id);

		index.setCrc(container.crc);
	}

	@Override
	public byte[] load(int index, int archive)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void store(int index, int archive, byte[] data)
	{
		int key = cacheDao.insertData(data);
		dataIds.put((long) index << 32 | archive, key);
	}
}
