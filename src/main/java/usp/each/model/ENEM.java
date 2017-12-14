package usp.each.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ENEM
{
    private static Set<Integer> yearsSaved = new HashSet<>();
    private List<String> columns;
    private String filePath;
    private int year;

    public ENEM(Collection<String> columns, String filePath, int year)
    {
        this.columns = new ArrayList<>();
        this.columns.addAll(columns);
        this.filePath = filePath;
        this.year = year;
    }

    public Dataset<Row> getDataset()
    {
        SparkSession spark = getSparkSession();
        
        if (!yearsSaved.contains(year))
        {
            System.out.println("criando view");
            createDataset();
        }

        return spark.table("enem"+year);
    }

    private void createDataset()
    {
        SparkSession spark = getSparkSession();

        String[] col = new String[columns.size() - 1];
        for (int i = 1; i < columns.size(); i++)
        {
            col[i - 1] = columns.get(i);
        }
        
        try
        {
            spark.read()
                .option("header", true)
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(filePath)
                .select(columns.get(0), col)
                .createOrReplaceTempView("enem"+year);
            
            new SQLContext(spark).cacheTable("enem"+year);
            
            yearsSaved.add(year);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    private SparkSession getSparkSession()
    {
        SparkConf conf = new SparkConf(true).setMaster("local[4]").setAppName("Teste para app!");
        return SparkSession.builder().config(conf).getOrCreate();
    }
}
