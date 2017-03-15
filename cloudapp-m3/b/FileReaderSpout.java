package coursera.b;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
    private SpoutOutputCollector _collector;
    private TopologyContext context;
    private BufferedReader reader;


    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
        try {
            this.reader = new BufferedReader(new FileReader(conf.get("file").toString()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.context = context;
        this._collector = collector;
    }

    @Override
    public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
        String value;
        try {
            while ((value = reader.readLine()) != null)
            _collector.emit(new Values(value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }

    @Override
    public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
