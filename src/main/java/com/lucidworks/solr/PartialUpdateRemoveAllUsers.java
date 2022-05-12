package com.lucidworks.solr;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PartialUpdateRemoveAllUsers {
  private static final Logger LOG = LoggerFactory.getLogger(PartialUpdateRemoveAllUsers.class);

  public static final int QUERY_NUM_ROWS = 5000;
  @Option(name = "-solrUrl", usage = "Solr url", required = true)
  private String solrUrl;

  @Option(name = "-collection", usage = "Solr collection", required = true)
  private String collection;

  @Option(name = "-fq", usage = "Solr fqs")
  private String[] fqs = new String[0];

  @Option(name = "-updatedMaxBatchSize", usage = "How many partial updates should each thread send to the collection?")
  private int updatedMaxBatchSize = 50;

  @Option(name = "-numThreads", usage = "How many threads should run updates in parallel?")
  private int numThreads = 5;

  @Option(name = "-commitWithin", usage = "How often should we commit?")
  private int commitWithin = 60000;

  @Option(name = "-countOnly", usage = "Count")
  private boolean countOnly = false;

  public static void main(String[] args) throws Exception {
    PartialUpdateRemoveAllUsers logExportUtility = new PartialUpdateRemoveAllUsers();
    CmdLineParser parser = new CmdLineParser(logExportUtility);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      parser.printUsage(System.out);
      return;
    }
    logExportUtility.run();
  }

  public void run() throws Exception {
    HttpSolrClient.Builder builder = new HttpSolrClient.Builder()
        .withBaseSolrUrl(solrUrl);
    try (SolrClient solrClient = builder.build();
         SolrBatchUpdateClient solrBatchPartialUpdateClient = new SolrBatchUpdateClient(builder, collection, updatedMaxBatchSize, numThreads, commitWithin)) {
      SolrQuery allUsersContent = new SolrQuery("*:*");
      allUsersContent.setFilterQueries(fqs);
      allUsersContent.setFields("id", "_lw_acl_ss");
      allUsersContent.setRows(QUERY_NUM_ROWS);
      allUsersContent.setSort("id", SolrQuery.ORDER.asc);
      String cursorMark = CursorMarkParams.CURSOR_MARK_START;
      long total = -1;
      boolean hasMore = true;
      while (hasMore) {
        allUsersContent.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        QueryResponse resp;
        resp = solrClient.query(collection, allUsersContent);
        if (total == -1) {
          total = resp.getResults().getNumFound();
          if (countOnly) {
            LOG.info("Count: {}", total);
            return;
          }
        }

        String nextCursorMark = resp.getNextCursorMark();
        if (cursorMark.equals(nextCursorMark)) {
          hasMore = false;
        }
        cursorMark = nextCursorMark;
        for (SolrDocument result : resp.getResults()) {
          List<Object> acls = Lists.newArrayList(result.getFieldValues("_lw_acl_ss"));
          acls.remove("all-users");
          solrBatchPartialUpdateClient.updateField((String) result.getFirstValue("id"), "_lw_acl_ss", acls);
        }
        LOG.info("Remaining to be sent to batch: {}", total -= resp.getResults().size());
      }
      LOG.info("Done. Running commit.");
    }
  }
}
