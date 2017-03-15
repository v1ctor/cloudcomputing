import java.util.HashMap;
import java.util.TreeSet;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
    private TreeSet<Pair<Integer, String>> set = new TreeSet<Pair<Integer, String>>();
    private int N;

    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);

        set.add(new Pair<>(count, word));
        currentTopWords.put(word, count);
        if (set.size() > N) {
            currentTopWords.remove(set.first().second);
            set.remove(set.first());
        }
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */


        //reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            collector.emit(new Values(printMap()));
            lastReportTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("top-N"));

    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]");
        return stringBuilder.toString();

    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
