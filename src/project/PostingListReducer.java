package project;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostingListReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

    	
        TreeMap<Integer, TreeSet<Integer>> docPositions = processIdAndPositions(values);

        StringBuilder result = getIdAndPositionsResult(docPositions);
        
        String keyFormatted = String.format("%-15s", key.toString());

        context.write(new Text(keyFormatted), new Text(result.toString()));
    }
    
    
    private  TreeMap<Integer, TreeSet<Integer>> processIdAndPositions(Iterable<Text> values)
    {
    	TreeMap<Integer, TreeSet<Integer>> docPositions = new TreeMap<>();
    	
    	 for (Text val : values) {
             String[] docAndPositions = val.toString().split(":");
             int docId = Integer.parseInt(docAndPositions[0]); 
             String[] positions = docAndPositions[1].split(",");

             TreeSet<Integer> positionsSet = docPositions.get(docId);
             if (positionsSet == null) {
                 positionsSet = new TreeSet<>();
                 docPositions.put(docId, positionsSet);
             }

             for (String pos : positions) {
                 positionsSet.add(Integer.parseInt(pos));
             }
         }
    	 
    	 return docPositions;
    }
    
    private  StringBuilder getIdAndPositionsResult(TreeMap<Integer, TreeSet<Integer>> docPositions)
    {
    	 StringBuilder result = new StringBuilder();
         for (Map.Entry<Integer, TreeSet<Integer>> entry : docPositions.entrySet()) {
             int docId = entry.getKey();
             TreeSet<Integer> positionsSet = entry.getValue();

             StringBuilder positionsBuilder = new StringBuilder();
             for (Integer pos : positionsSet) {
                 if (positionsBuilder.length() > 0) {
                     positionsBuilder.append(",");
                 }
                 positionsBuilder.append(pos);
             }

             
             if (result.length() > 0) {
                 result.append(";");
             }
             result.append(docId).append(":").append(positionsBuilder.toString());
         }
         
         return result;
    }
}