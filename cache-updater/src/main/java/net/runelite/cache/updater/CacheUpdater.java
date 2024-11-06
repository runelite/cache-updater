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
import java.time.Instant;
import java.util.List;
import net.runelite.cache.client.CacheClient;
import net.runelite.cache.client.IndexInfo;
import net.runelite.cache.fs.Store;
import net.runelite.cache.updater.beans.ArchiveEntry;
import net.runelite.cache.updater.beans.CacheEntry;
import net.runelite.protocol.api.login.HandshakeResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

@SpringBootApplication
public class CacheUpdater implements CommandLineRunner
{
	private static final Logger logger = LoggerFactory.getLogger(CacheUpdater.class);

	private final Sql2o sql2o;

	@Value("${rs.version}")
	private int rsVersion;

	@Value("${rs.host}")
	private String host;

	@Value("${rs.port:43594}")
	private int port;

	@Autowired
	public CacheUpdater(
		@Qualifier("RuneLite Cache SQL2O") Sql2o sql2o
	)
	{
		this.sql2o = sql2o;
	}

	public void update() throws IOException
	{
		try (Connection con = sql2o.beginTransaction())
		{
			CacheDAO cacheDao = new CacheDAO(con);
			CacheEntry cache = cacheDao.findMostRecent();
			boolean created = false;
			if (cache == null)
			{
				created = true;
				cache = cacheDao.createCache(rsVersion, Instant.now());
			}

			CacheStorage storage = new CacheStorage(cache, cacheDao);
			Store store = new Store(storage);
			store.load();

			CacheClient client = new CacheClient(store, host, port, rsVersion);

			client.connect();
			HandshakeResponseType result = client.handshake().join();

			if (result != HandshakeResponseType.RESPONSE_OK)
			{
				logger.warn("Out of date!");
				return;
			}

			List<IndexInfo> indexes = client.requestIndexes();
			List<ArchiveEntry> entries = cacheDao.findIndexesForCache(cache);

			if (!checkOutOfDate(indexes, entries))
			{
				logger.info("All up to date.");
				return;
			}

			client.download();

			CacheEntry newCache = created ? cache : cacheDao.createCache(rsVersion, Instant.now());
			storage.setCacheEntry(newCache);

			logger.info("Saving new cache");

			store.save();

			// commit database
			con.commit();

			logger.info("Done!");
		}
	}

	private boolean checkOutOfDate(List<IndexInfo> indexes, List<ArchiveEntry> dbIndexes)
	{
		if (indexes.size() != dbIndexes.size())
		{
			return true;
		}

		for (int i = 0; i < indexes.size(); ++i)
		{
			IndexInfo ii = indexes.get(i);
			ArchiveEntry ie = dbIndexes.get(i);

			if (ii.getId() != ie.getIndexId()
				|| ii.getRevision() != ie.getRevision()
				|| ii.getCrc() != ie.getCrc())
			{
				return true;
			}
		}

		return false;
	}

	@Override
	public void run(String... args) throws Exception
	{
		update();
	}

	public static void main(String[] args)
	{
		SpringApplication.run(CacheUpdater.class, args).close();
		System.exit(0);
	}

	static
	{
		// this is required so that the re-compressed index data crc matches Jagex's
		System.setProperty("runelite.useNativeBzip", "true");
	}

}
