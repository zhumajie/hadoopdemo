import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class STJoin extends MapReduce{

    public static class Map extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            String line = value.toString();
            if (line.indexOf("child") > -1 || StringUtils.isEmpty(line)) {
                return;
            }
            int i = 0;
            while(line.charAt(i) != ' ') {
                i ++;
            }
            String[] values = {line.substring(0, i), line.substring(i+1)};
            context.write(new Text(values[1]), new Text("1+" + values[0] + "+" + values[1]));
            context.write(new Text(values[0]), new Text("2+" + values[0] + "+" + values[1]));
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private static int time = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparend"));
                time ++;
            }
            int grandchildnum = 0, grandparennum = 0;
            String[] grandchild = new String[10], grandparent = new String[10];
            Iterator<Text> ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString();
                int len = record.length();
                int i = 2;
                if (len == 0) continue;
                char relationType = record.charAt(0);
                String child = new String();
                String parent = new String();
                while (record.charAt(i) != '+') {
                    child += record.charAt(i);
                    i ++;
                }
                i += 1;
                while (i < len) {
                    parent += record.charAt(i);
                    i ++;
                }
                if (relationType == '1') {
                    grandchild[grandchildnum] = child;
                    grandchildnum++;
                } else {
                    grandparent[grandparennum] = parent;
                    grandparennum++;
                }
                if (grandchildnum > 0 && grandparennum > 0) {
                    for (int m = 0; m < grandchildnum; m ++) {
                        for(int n = 0; n < grandparennum; n ++) {
                            context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                        }
                    }
                }
            }

        }

    }

    public static void main(String[] args){
        start("single table join", STJoin.class, Map.class, Reduce.class, Text.class, Text.class, args);
    }
}
