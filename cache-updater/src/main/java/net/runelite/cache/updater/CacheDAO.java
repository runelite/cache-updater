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

import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import net.runelite.cache.updater.beans.ArchiveEntry;
import net.runelite.cache.updater.beans.CacheEntry;
import org.sql2o.Connection;
import org.sql2o.Query;

@RequiredArgsConstructor
class CacheDAO
{
	private final Connection conn;
	// cache prepared statements for high volume queries
	private Query findArchive;
	private Query linkArchive;
	private Query insertArchive;
	private Query insertData;

	CacheEntry createCache(int revision, Instant date)
	{
		int cacheId = conn.createQuery("insert into cache (revision, date) values (:revision, :date)")
			.addParameter("revision", revision)
			.addParameter("date", date)
			.executeUpdate()
			.getKey(int.class);

		CacheEntry entry = new CacheEntry();
		entry.setId(cacheId);
		entry.setRevision(revision);
		entry.setDate(date);
		return entry;
	}

	CacheEntry findMostRecent()
	{
		return conn.createQuery("select id, revision, date from cache order by revision desc, date desc limit 1")
			.executeAndFetchFirst(CacheEntry.class);
	}

	List<ArchiveEntry> findIndexesForCache(CacheEntry cache)
	{
		return conn.createQuery("select a.id, a.index, a.archive, a.crc, a.name, a.revision, a.data_id from cache_archive ca " +
			"join archive a on ca.archive_id = a.id where ca.cache_id = :cache_id and a.index = :index_id")
			.addParameter("cache_id", cache.getId())
			.addParameter("index_id", 255)
			.addColumnMapping("index", "indexId")
			.addColumnMapping("archive", "archiveId")
			.addColumnMapping("data_id", "dataId")
			.executeAndFetch(ArchiveEntry.class);
	}

	int findArchive(int index, int archive, int crc, int name, int rev)
	{
		if (findArchive == null)
		{
			findArchive = conn.createQuery("SELECT a.id, a.index, a.archive, a.crc, a.revision, a.name from archive a " +
				"where a.index = :index and a.archive = :archive and a.crc = :crc and a.revision = :revision and a.name = :name");
		}
		ArchiveEntry archiveEntry = findArchive
			.addParameter("index", index)
			.addParameter("archive", archive)
			.addParameter("crc", crc)
			.addParameter("revision", rev)
			.addParameter("name", name)
			.addColumnMapping("index", "indexId")
			.addColumnMapping("archive", "archiveId")
			.executeAndFetchFirst(ArchiveEntry.class);
		return archiveEntry == null ? -1 : archiveEntry.getId();
	}

	void linkArchive(int cacheId, int archiveId)
	{
		if (linkArchive == null)
		{
			linkArchive = conn.createQuery("INSERT INTO cache_archive (cache_id, archive_id) " +
				"VALUES (:cache_id, :archive_id)");
		}
		linkArchive
			.addParameter("cache_id", cacheId)
			.addParameter("archive_id", archiveId)
			.executeUpdate();
	}

	int insertArchive(int indexId, int archiveId, int crc, int name, int revision, int dataId)
	{
		if (insertArchive == null)
		{
			insertArchive = conn.createQuery("INSERT INTO archive (`index`, archive, crc, revision, name, data_id) " +
				"VALUES (:index, :archive, :crc, :revision, :name, :data_id)");
		}
		return insertArchive
			.addParameter("index", indexId)
			.addParameter("archive", archiveId)
			.addParameter("crc", crc)
			.addParameter("revision", revision)
			.addParameter("name", name)
			.addParameter("data_id", dataId)
			.executeUpdate()
			.getKey(int.class);
	}

	int insertData(byte[] data)
	{
		if (insertData == null)
		{
			insertData = conn.createQuery("INSERT INTO data (data) VALUES (:data)");
		}
		return insertData
			.addParameter("data", data)
			.executeUpdate()
			.getKey(int.class);
	}

	byte[] readData(int id)
	{
		return conn.createQuery("SELECT data FROM data WHERE id = :id")
			.addParameter("id", id)
			.executeAndFetchFirst(byte[].class);
	}
}
