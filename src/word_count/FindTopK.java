package word_count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author yifengguo
 */
public class FindTopK {
    public static void main(String[] main) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/home/yifengguo/IdeaProjects/Hadoop-Study/wordcount1_output/part-r-00000"));
            String line = "";
            Map<String, Integer> map = new HashMap<>();
            while ((line = br.readLine()) != null) {
                String[] data = line.split("\t");
                map.put(data[0], Integer.valueOf(data[1]));
            }
            br.close();
            int n = 21;
            List<Map.Entry<String, Integer>> greatest = findGreatest(map, n);
            System.out.println("Top "+ n +" entries:");
            for (Map.Entry<String, Integer> entry : greatest)
            {
                System.out.println(entry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>>
    findGreatest(Map<K, V> map, int n)
    {
        Comparator<? super Map.Entry<K, V>> comparator = new Comparator<Map.Entry<K, V>>()
        {
            @Override
            public int compare(Map.Entry<K, V> e0, Map.Entry<K, V> e1)
            {
                V v0 = e0.getValue();
                V v1 = e1.getValue();
                return v0.compareTo(v1);
            }
        };
        PriorityQueue<Map.Entry<K, V>> highest =
                new PriorityQueue<Map.Entry<K,V>>(n, comparator);
        for (Map.Entry<K, V> entry : map.entrySet())
        {
            highest.offer(entry);
            while (highest.size() > n)
            {
                highest.poll();
            }
        }

        List<Map.Entry<K, V>> result = new ArrayList<Map.Entry<K,V>>();
        while (highest.size() > 0)
        {
            result.add(highest.poll());
        }
        Collections.reverse(result);
        return result;
    }
}
