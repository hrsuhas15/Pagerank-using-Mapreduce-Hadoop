

package it.uniroma1.hadoop.pagerank.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.uniroma1.hadoop.pagerank.PageRank;

import java.io.IOException;

public class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
        String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
        
        String[] allOtherPages = links.split(",");
        for (String otherPage : allOtherPages) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        
        // put the original links so the reducer is able to produce the correct output
        context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
        
    }
    
}

