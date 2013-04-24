package panda.core;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;


@SuppressWarnings("static-method")
public class PacketCacheTest
{
	@Test
	public void testAdd()
	{
		PacketCache cache = new PacketCache(3);
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 1 }, 1L);
		Assert.assertEquals(1L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 2 }, 2L);
		Assert.assertEquals(1L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(2L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(0L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 3 }, 3L);
		Assert.assertEquals(1L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(2L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(3L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 4 }, 4L);
		Assert.assertEquals(4L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(2L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(3L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 5 }, 5L);
		Assert.assertEquals(4L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(5L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(3L, cache.getSequenceNumberOfIndex(2));

		cache.add(new byte[] { 6 }, 6L);
		Assert.assertEquals(4L, cache.getSequenceNumberOfIndex(0));
		Assert.assertEquals(5L, cache.getSequenceNumberOfIndex(1));
		Assert.assertEquals(6L, cache.getSequenceNumberOfIndex(2));
	}

	@Test
	public void testGetCachedPackets()
	{
		PacketCache cache = new PacketCache(3);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 2L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 1 }, 1L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), Integer.valueOf(1), 1L);
		assertPair(cache.getCachedPackets(1L, 2L), Integer.valueOf(1), 1L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(1), 1L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 2 }, 2L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), Integer.valueOf(1), 1L);
		assertPair(cache.getCachedPackets(1L, 2L), Integer.valueOf(2), 1L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(2), 1L);
		assertPair(cache.getCachedPackets(2L, 3L), Integer.valueOf(1), 2L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 3 }, 3L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), Integer.valueOf(1), 1L);
		assertPair(cache.getCachedPackets(1L, 2L), Integer.valueOf(2), 1L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(3), 1L);
		assertPair(cache.getCachedPackets(2L, 3L), Integer.valueOf(2), 2L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 4 }, 4L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 2L), Integer.valueOf(1), 2L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(3), 2L);
		assertPair(cache.getCachedPackets(2L, 3L), Integer.valueOf(2), 2L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 5 }, 5L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 2L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(3), 3L);
		assertPair(cache.getCachedPackets(2L, 3L), Integer.valueOf(1), 3L);
		assertPair(cache.getCachedPackets(6L, 6L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 7L), null, 0L);

		cache.add(new byte[] { 6 }, 6L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 2L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(3), 4L);
		assertPair(cache.getCachedPackets(2L, 3L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 6L), Integer.valueOf(1), 6L);
		assertPair(cache.getCachedPackets(6L, 7L), Integer.valueOf(1), 6L);

		cache.add(new byte[] { 7 }, 7L);
		assertPair(cache.getCachedPackets(0L, 0L), null, 0L);
		assertPair(cache.getCachedPackets(0L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 1L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 2L), null, 0L);
		assertPair(cache.getCachedPackets(1L, 6L), Integer.valueOf(2), 5L);
		assertPair(cache.getCachedPackets(2L, 3L), null, 0L);
		assertPair(cache.getCachedPackets(6L, 6L), Integer.valueOf(1), 6L);
		assertPair(cache.getCachedPackets(6L, 7L), Integer.valueOf(2), 6L);
	}

	private void assertPair(Pair<List<byte[]>, Long> cachedPackets, Integer count, long firstSequenceNumber)
	{
		if (count == null)
		{
			Assert.assertNull(cachedPackets);
		}
		else
		{
			Assert.assertEquals(count.intValue(), cachedPackets.getA().size());
			Assert.assertEquals(firstSequenceNumber, cachedPackets.getB().longValue());
			Assert.assertEquals((byte) (int) firstSequenceNumber, cachedPackets.getA().get(0)[0]);
		}
	}
}