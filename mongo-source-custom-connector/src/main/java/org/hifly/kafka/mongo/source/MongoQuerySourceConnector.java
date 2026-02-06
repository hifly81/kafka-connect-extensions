package org.hifly.kafka.mongo.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoQuerySourceConnector extends SourceConnector {

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        MongoQuerySourceConfig config = new MongoQuerySourceConfig(props);
        this.configProps = new HashMap<>();
        for (Map.Entry<String, Object> e : config.originals().entrySet()) {
            this.configProps.put(e.getKey(), String.valueOf(e.getValue()));
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoQuerySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // 1 task 1 collection
        List<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(new HashMap<>(configProps));
        return configs;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return MongoQuerySourceConfig.configDef();
    }
}