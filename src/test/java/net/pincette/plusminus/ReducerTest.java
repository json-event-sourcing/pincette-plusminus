package net.pincette.plusminus;

import static net.pincette.jes.test.Test.run;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Streams.fromConfig;
import static net.pincette.plusminus.Application.createApp;
import static net.pincette.plusminus.Application.getMongoClient;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import java.io.File;
import org.junit.jupiter.api.Test;

class ReducerTest {
  private static final String ENVIRONMENT = "environment";
  private static final String KAFKA = "kafka";

  @Test
  void test() {
    final Config config = loadDefault();

    tryToDoWithRethrow(
        () -> getMongoClient(config),
        client ->
            assertTrue(
                run(
                    new File("src/test/resources").toPath(),
                    fromConfig(config, KAFKA),
                    config.getString(ENVIRONMENT),
                    net.pincette.jes.test.Application::report,
                    builder -> createApp(builder, config, client))));
  }
}
