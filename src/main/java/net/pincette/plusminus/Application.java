package net.pincette.plusminus;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.lang.System.exit;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.elastic.APM.monitor;
import static net.pincette.jes.elastic.Logging.log;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Streams.start;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import java.util.concurrent.CompletionStage;
import java.util.function.IntUnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.jes.Aggregate;
import net.pincette.jes.util.Fanout;
import net.pincette.jes.util.Streams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * A demo application for pincette-jes.
 *
 * @author Werner Donn\u00e9
 */
public class Application {
  private static final String AUTH = "authorizationHeader";
  private static final String ENVIRONMENT = "environment";
  private static final String KAFKA = "kafka";
  private static final String LOG_LEVEL = "logLevel";
  private static final String URI = "uri";
  private static final String VALUE = "value";
  private static final String VERSION = "1.0";

  private static CompletionStage<JsonObject> reduce(
      final JsonObject currentState, final IntUnaryOperator op) {
    return completedFuture(
        createObjectBuilder(currentState)
            .add(VALUE, op.applyAsInt(currentState.getInt(VALUE, 0)))
            .build());
  }

  public static void main(final String[] args) {
    final StreamsBuilder builder = new StreamsBuilder();
    final Config config = loadDefault();
    final String environment = tryToGetSilent(() -> config.getString(ENVIRONMENT)).orElse("dev");
    final Level logLevel = parse(tryToGetSilent(() -> config.getString(LOG_LEVEL)).orElse("INFO"));
    final Logger logger = getLogger("plusminus");

    tryToDoWithRethrow(
        () -> create(config.getString("mongodb.uri")),
        client -> {
          final Aggregate aggregate =
              new Aggregate()
                  .withApp("plusminus")
                  .withType("counter")
                  .withEnvironment(environment)
                  .withBuilder(builder)
                  .withMongoDatabase(client.getDatabase(config.getString("mongodb.database")))
                  .withBreakingTheGlass()
                  .withMonitoring(true)
                  .withAudit("audit-dev")
                  .withReducer("plus", (command, currentState) -> reduce(currentState, v -> v + 1))
                  .withReducer(
                      "minus", (command, currentState) -> reduce(currentState, v -> v - 1));

          aggregate.build();

          tryToGetSilent(() -> config.getConfig("fanout"))
              .ifPresent(
                  fanout ->
                      Fanout.connect(
                          aggregate.replies(),
                          fanout.getString("realmId"),
                          fanout.getString("realmKey")));

          tryToGetSilent(() -> config.getConfig("elastic.apm"))
              .ifPresent(apm -> monitor(aggregate, config.getString(URI), config.getString(AUTH)));

          tryToGetSilent(() -> config.getConfig("elastic.log"))
              .ifPresent(
                  log -> {
                    log(aggregate, logLevel, VERSION, log.getString(URI), log.getString(AUTH));

                    log(
                        logger,
                        logLevel,
                        VERSION,
                        environment,
                        log.getString(URI),
                        log.getString(AUTH));
                  });

          final Topology topology = builder.build();

          logger.log(Level.INFO, "Topology:\n\n {0}", topology.describe());

          if (!start(topology, Streams.fromConfig(config, KAFKA))) {
            exit(1);
          }
        });
  }
}
