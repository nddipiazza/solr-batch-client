package com.lucidworks.solr;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * A Solr client capable of doing things in batches - keeps plumbing code out of the
 * main dist.
 */
public class SolrBatchUpdateClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SolrBatchUpdateClient.class);

  public static final int QUEUE_CAPACITY = 20_000;
  public static final int SLEEP_BETWEEN_POLLS_MS = 5000;

  private final CloudSolrClient.Builder cloudSolrClientBuilder;
  private final HttpSolrClient.Builder httpSolrClientBuilder;
  private final int batchSize;
  private final String collection;
  private final ExecutorService processorEs;
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final AtomicLong totalDocsUpdated = new AtomicLong(0);
  private final int commitWithin;

  private final BlockingQueue<SolrInputDocument> processQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

  private class SolrBatchProcessor implements Runnable {
    private final int threadIdx;

    public SolrBatchProcessor(int threadIdx) {
      this.threadIdx = threadIdx;
    }

    public SolrClient newClient() {
      if (cloudSolrClientBuilder != null) {
        return cloudSolrClientBuilder.build();
      } else if (httpSolrClientBuilder != null) {
        return httpSolrClientBuilder.build();
      } else {
        throw new RuntimeException("Could not create client - unsupported solr client type");
      }
    }

    @Override
    public void run() {
      try (SolrClient solrClient = newClient()) {
        AtomicLong jobUpdated = new AtomicLong(0);
        while (!done.get()) {
          Set<SolrInputDocument> nextBatch = Sets.newHashSet();
          processQueue.drainTo(nextBatch, batchSize);
          Stopwatch sw = Stopwatch.createStarted();
          if (!nextBatch.isEmpty()) {
            try {
              solrClient.add(collection, nextBatch, commitWithin);
            } catch (Exception e) {
              LOG.error("Could not process next batch", e);
            }
          }
          if (nextBatch.isEmpty()) {
            // prevent hard cpu usage by waiting when you got nothing from asking for docs.
            try {
              Thread.sleep(SLEEP_BETWEEN_POLLS_MS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          } else {
            LOG.info("Next batch - processorThreadIdx={}, numInBatch={}, took={}, totalUpdatedSoFar={}, thisJobUpdatedSoFar={}",
                threadIdx, nextBatch.size(), sw.elapsed(), totalDocsUpdated.addAndGet(nextBatch.size()), jobUpdated.addAndGet(nextBatch.size()));
          }
        }
        LOG.info("Processor is done - processorThreadIdx={}", threadIdx);
      } catch (Exception e) {
        LOG.error("Could not run solr batch client", e);
        throw new RuntimeException(e);
      }
    }
  }

  public SolrBatchUpdateClient(CloudSolrClient.Builder cloudSolrClientBuilder, String collection, int updateMaxBatchSize, int numThreads, int commitWithin) {
    this.cloudSolrClientBuilder = cloudSolrClientBuilder;
    this.httpSolrClientBuilder = null;
    this.batchSize = updateMaxBatchSize;
    this.collection = collection;
    this.commitWithin = commitWithin;
    assert numThreads > 0 && numThreads < 100;
    processorEs = Executors.newFixedThreadPool(numThreads);
    IntStream.rangeClosed(1, numThreads)
        .forEach((idx) -> processorEs.execute(new SolrBatchProcessor(idx)));
  }


  public SolrBatchUpdateClient(HttpSolrClient.Builder httpSolrClientBuilder, String collection, int updateMaxBatchSize, int numThreads, int commitWithin) {
    this.cloudSolrClientBuilder = null;
    this.httpSolrClientBuilder = httpSolrClientBuilder;
    this.batchSize = updateMaxBatchSize;
    this.collection = collection;
    assert numThreads > 0 && numThreads < 100;
    this.commitWithin = commitWithin;
    processorEs = Executors.newFixedThreadPool(numThreads);
    IntStream.rangeClosed(1, numThreads)
        .forEach((idx) -> processorEs.execute(new SolrBatchProcessor(idx)));
  }

  public void updateField(String id, String field, Object value) {
    SolrInputDocument updateDoc = new SolrInputDocument();
    updateDoc.addField("id", id);
    updateDoc.setField(field, Collections.singletonMap("set", value));
    try {
      processQueue.put(updateDoc);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    if (!processorEs.isShutdown()) {
      LOG.info("Waiting for final batches to submit...");
      done.set(true);
      processorEs.shutdown();
    }
  }
}
