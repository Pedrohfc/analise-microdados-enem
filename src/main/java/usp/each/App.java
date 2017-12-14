package usp.each;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JLabel;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import usp.each.controller.DashboardController;

public class App
{
    public static Map<String, String> valuesMap;
    public static Set<String> columns;
    
    public static void main(String[] args) throws Exception
    {
        init(); // inicia configuracoes
        
        // inicia spark
        SparkConf conf = new SparkConf(true).setMaster("local[4]").setAppName("Teste para app!");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping");
            spark.stop();
        }));
        
        // ler o arquivo csv
        String filePath = "/home/pedro/Documents/Microdados_enem_2016/DADOS/microdados_enem_2016.csv";
        //String filePath = "resources/teste.csv";
        DashboardController dash = new DashboardController(filePath);
        int year = 2016;
        dash.mediaPorRendaFamiliar(year);
        //String[] columns = {"NU_NOTA_REDACAO", "NU_NOTA_MT", "NU_NOTA_LC", "NU_NOTA_CH", "NU_NOTA_CN"};
        String[] columns = {"NU_NOTA_REDACAO"};
        for (String prova : columns)
        {
            dash.mediaProvaPorRendaFamiliar(prova, year);
            dash.mediaPorProvaPorTipoEscola(prova, year);
        }
        
        
        // APENAS PRA TESTE
        JFrame meFeche = new JFrame("Me feche");
        JLabel meFecheLabel = new JLabel("Me feche por favor");
        meFeche.add(meFecheLabel);
        meFeche.setSize(640, 480);
        meFeche.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        meFeche.setVisible(true);
    }
    
    public static void init() throws Exception
    {
        valuesMap = new HashMap<>();
        File configFile = new File("resources/valuesMap.txt");
        BufferedReader valuesIn = new BufferedReader(new FileReader(configFile));
        while (valuesIn.ready())
        {
            String[] line = valuesIn.readLine().split(";");
            valuesMap.put(line[0], line[1]);
        }
        valuesIn.close();
        
        
        File columnsConfig = new File("resources/columns.txt");
        BufferedReader columnsIn = new BufferedReader(new FileReader(columnsConfig));
        columns = new HashSet<>();
        
        while (columnsIn.ready())
        {
            columns.add(columnsIn.readLine());
        }
        
        columnsIn.close();
    }
}
