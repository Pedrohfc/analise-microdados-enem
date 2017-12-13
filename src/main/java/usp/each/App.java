package usp.each;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;

public class App
{
    public static Map<String, String> valuesMap;
    
    public static void main(String[] args) throws Exception
    {
        init(); // inicia configuracoes
        
        // inicia spark
        SparkConf conf = new SparkConf(true).setMaster("local[4]").setAppName("Teste para app!");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        
        // ler o arquivo csv
        String filePath = "/home/pedro/Documents/Microdados_enem_2016/DADOS/microdados_enem_2016.csv";
        //String filePath = "resources/teste.csv";
        Dataset<Row> enem = spark.read()
                                .option("header", true)
                                .option("delimiter", ";")
                                .option("inferSchema", "true")
                                .csv(filePath);
        
        enem.printSchema(); // imprime o esquema do dataset
        
        // query sobre a media de notas por renda familiar (Q006)
        List<Row> result = enem.groupBy("Q006")
                                .avg("NU_NOTA_REDACAO", "NU_NOTA_MT", "NU_NOTA_LC", "NU_NOTA_CH", "NU_NOTA_CN")
                                .orderBy("Q006")
                                .collectAsList();
        
        // Legenda do graficos
        Map<String, String> prova = new HashMap<>();
        prova.put("avg(NU_NOTA_REDACAO)", "Redação");
        prova.put("avg(NU_NOTA_MT)", "Matemática");
        prova.put("avg(NU_NOTA_LC)", "Linguagem");
        prova.put("avg(NU_NOTA_CH)", "Humanas");
        prova.put("avg(NU_NOTA_CN)", "Ciências");
        
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        // Transforma resultada da query para dataset dos graficos
        for (Row row : result)
        {
            for (String column : row.schema().fieldNames())
            {
                if (!column.equals("Q006") && valuesMap.get(row.getAs("Q006")) != null)
                {
                    dataset.addValue(row.getAs(column), valuesMap.get(row.getAs("Q006")), prova.get(column));
                }
            }
        }
        
        // Cria o grafico
        JFreeChart chart = ChartFactory.createBarChart("Média da nota do ENEM por Renda Familiar", "Renda", "Nota", dataset);
        
        // Salva o grafico como JPG
        ChartUtils.saveChartAsJPEG(new File("resources/nota_renda.jpg"), chart, 640, 480);
        
        // Cria janela para exebicao do grafico
        ChartPanel panel = new ChartPanel(chart);
        
        JFrame frame = new JFrame("Analise ENEM");
        frame.add(panel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        
        spark.stop();
    }
    
    public static void init() throws Exception
    {
        valuesMap = new HashMap<>();
        File configFile = new File("resources/valuesMap.txt");
        BufferedReader in = new BufferedReader(new FileReader(configFile));
        while (in.ready())
        {
            String[] line = in.readLine().split(";");
            valuesMap.put(line[0], line[1]);
        }
        in.close();
    }
}
