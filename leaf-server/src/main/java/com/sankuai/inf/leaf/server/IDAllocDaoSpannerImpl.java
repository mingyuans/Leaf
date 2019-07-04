package com.sankuai.inf.leaf.server;

import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;

/**
 * CREATE TABLE leaf_alloc (
 * 	biz_tag STRING(128),
 * 	description STRING(256),
 * 	max_id INT64 NOT NULL,
 * 	step INT64 NOT NULL,
 * 	update_time TIMESTAMP NOT NULL,
 * ) PRIMARY KEY (biz_tag)
 */
public class IDAllocDaoSpannerImpl implements IDAllocDao {

    private Logger logger = LoggerFactory.getLogger(IDAllocDaoSpannerImpl.class);

    private DatabaseClient mDbClient;

    public IDAllocDaoSpannerImpl() {
        // Instantiates a client
        SpannerOptions options = SpannerOptions.newBuilder()
                .setProjectId("aftership-test")
                .build();
        Spanner spanner = options.getService();

        // Name of your instance & database.
        String instanceId = "aftership-test-1";
        String databaseId = "example-db";

        // Creates a database client
        mDbClient = spanner.getDatabaseClient(
                DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    }

    @Override
    public List<LeafAlloc> getAllLeafAllocs() {
        logger.info("getAllLeafAllocs");
        try {
            // Queries the database
            ResultSet resultSet = mDbClient.singleUse()
                    .executeQuery(Statement.of("SELECT biz_tag, max_id, step, update_time FROM leaf_alloc"));

            List<LeafAlloc> allocs = new LinkedList<>();
            // Prints the results
            while (resultSet.next()) {
                LeafAlloc alloc = new LeafAlloc();
                alloc.setKey(resultSet.getString("biz_tag"));
                alloc.setMaxId(resultSet.getLong("max_id"));
                alloc.setStep((int) resultSet.getLong("step"));
                alloc.setUpdateTime(resultSet.getTimestamp("update_time").toString());
                logger.info(alloc.toString());
            }

            return allocs;
        } catch (Throwable throwable) {
            logger.error("getAllLeafAllocs", throwable);
        }
        return new LinkedList<>();
    }

    @Override
    public LeafAlloc updateMaxIdAndGetLeafAlloc(final String tag) {
        try {
            return mDbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<LeafAlloc>() {
                @Nullable
                @Override
                public LeafAlloc run(TransactionContext transactionContext) throws Exception {
                    Statement statement = Statement.newBuilder("UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag =@tag")
                            .bind("tag").to(tag).build();
                    long timestamp = transactionContext.executeUpdate(statement);
                    logger.info("updateMaxIdAndGetLeafAlloc, timestamp: " + timestamp);

                    Struct struct = transactionContext.readRow("leaf_alloc",Key.of(tag),
                            ImmutableList.<String>of("biz_tag","max_id","step","update_time"));

                    LeafAlloc alloc = new LeafAlloc();
                    alloc.setKey(struct.getString("biz_tag"));
                    alloc.setMaxId(struct.getLong("max_id"));
                    alloc.setStep((int) struct.getLong("step"));
                    alloc.setUpdateTime(struct.getTimestamp("update_time").toString());

                    return alloc;
                }
            });
        } catch (Throwable throwable) {
            logger.error("updateMaxIdAndGetLeafAlloc", throwable);
        }

        return null;
    }

    @Override
    public LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(final LeafAlloc leafAlloc) {
        try {
            return mDbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<LeafAlloc>() {
                @Nullable
                @Override
                public LeafAlloc run(TransactionContext transactionContext) throws Exception {
                    Statement statement = Statement.newBuilder("UPDATE leaf_alloc SET max_id = max_id +@step WHERE biz_tag =@key")
                            .bind("step").to(leafAlloc.getStep()).bind("key").to(leafAlloc.getKey())
                            .build();
                    long timestamp = transactionContext.executeUpdate(statement);
                    logger.info("updateMaxIdAndGetLeafAlloc, timestamp: " + timestamp);

                    Struct struct = transactionContext.readRow("leaf_alloc",Key.of(leafAlloc.getKey()),
                            ImmutableList.<String>of("biz_tag","max_id","step","update_time"));

                    LeafAlloc alloc = new LeafAlloc();
                    alloc.setKey(struct.getString("biz_tag"));
                    alloc.setMaxId(struct.getLong("max_id"));
                    alloc.setStep((int) struct.getLong("step"));
                    alloc.setUpdateTime(struct.getTimestamp("update_time").toString());

                    return alloc;
                }
            });
        } catch (Throwable throwable) {
            logger.error("updateMaxIdAndGetLeafAlloc", throwable);
        }

        return null;
    }

    @Override
    public List<String> getAllTags() {
        logger.info("getAllTags");
        try {
            // Queries the database
            ResultSet resultSet = mDbClient.singleUse()
                    .executeQuery(Statement.of("SELECT biz_tag FROM leaf_alloc"));

            List<String> tags = new LinkedList<>();
            // Prints the results
            while (resultSet.next()) {
                tags.add(resultSet.getString("biz_tag"));
            }

            return tags;
        } catch (Throwable throwable) {
            logger.error("getAllTags", throwable);
        }
        return new LinkedList<>();
    }
}
