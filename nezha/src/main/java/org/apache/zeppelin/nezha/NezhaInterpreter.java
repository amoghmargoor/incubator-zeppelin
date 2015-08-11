package org.apache.zeppelin.nezha;

import com.qubole.qds.sdk.java.client.DefaultQdsConfiguration;
import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.QdsClientFactory;
import com.qubole.qds.sdk.java.client.QdsConfiguration;
import com.qubole.qds.sdk.java.client.ResultLatch;
import com.qubole.qds.sdk.java.entities.CommandResponse;
import com.qubole.qds.sdk.java.entities.ResultValue;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Created by dev on 8/6/15.
 */
public class NezhaInterpreter extends Interpreter {

  static final Logger LOGGER = LoggerFactory.getLogger(NezhaInterpreter.class);

  static final String QUBOLE_ENDPOINT = "qubole.endpoint";
  static final String QUBOLE_API_KEY = "qubole.api.key";

  static final String DEFAULT_CREDENTIAL =
      "None";
  static final String DEFAULT_ENDPOINT = "https://v2.qubole.net/api/v2/commands";

  static {
    LOGGER.info("Bootstrapping Nezha Interpreter");
    Interpreter.register("nezha", "nezha", NezhaInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add(QUBOLE_ENDPOINT, DEFAULT_ENDPOINT,
                "Default = https://v2.qubole.net/api/v2/commands")
            .add(QUBOLE_API_KEY, DEFAULT_CREDENTIAL,
                "Default API Key = " +
                    "None")
            .build());
  }

  public NezhaInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    LOGGER.info("Input String: " + st);
    InterpreterResult result;
    String endpoint = getProperty(QUBOLE_ENDPOINT);
    LOGGER.info("Qubole endpoint: " + endpoint);
    QdsConfiguration configuration =
        new DefaultQdsConfiguration(endpoint, getProperty(QUBOLE_API_KEY));
    QdsClient client = QdsClientFactory.newClient(configuration);
    try
    {
      CommandResponse commandResponse;
      String title = context.getParagraphTitle();
      if (title != null && title.toLowerCase().startsWith("hive")) {
        LOGGER.info("Hive Command being Executed");
        commandResponse =
            client.command().hive().query(st).invoke().get();
      } else {
        LOGGER.info("Sql Command being Executed");
        commandResponse =
            client.command().sql().query(st).invoke().get();
      }
      ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
      ResultValue resultValue = resultLatch.awaitResult();
      final String resultValueResults = resultValue.getResults();
      result =
          new InterpreterResult(InterpreterResult.Code.SUCCESS,
          "%table " + resultValueResults);
    } catch (Exception e) {
      result = new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
    finally
    {
      client.close();
    }
    return result;
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        NezhaInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }
}
