package usp.each.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;

import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import usp.each.App;
import usp.each.model.ENEM;

public class DashboardController
{
    private String filePath;
    private Map<String, String> prova = new HashMap<>();
    
    public DashboardController(String filePath)
    {
        this.filePath = filePath;
        
        prova.put("avg(NU_NOTA_REDACAO)", "Redação");
        prova.put("avg(NU_NOTA_MT)", "Matemática");
        prova.put("avg(NU_NOTA_LC)", "Linguagem");
        prova.put("avg(NU_NOTA_CH)", "Humanas");
        prova.put("avg(NU_NOTA_CN)", "Ciências");
        prova.put("NU_NOTA_REDACAO", "Redação");
        prova.put("NU_NOTA_MT", "Matemática");
        prova.put("NU_NOTA_LC", "Linguagem");
        prova.put("NU_NOTA_CH", "Humanas");
        prova.put("NU_NOTA_CN", "Ciências");
    }
    
    public List<Row> avg(String group, String[] columns, int year)
    {
        List<Row> result = null;
        ENEM data = new ENEM(App.columns, filePath, year);
        
        result = data.getDataset().groupBy(group)
                                    .avg(columns)
                                    .orderBy(group)
                                    .collectAsList();
        return result;
    }
    
    public CategoryDataset listRowToDataset(List<Row> result, String group)
    {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        // Transforma resultada da query para dataset dos graficos
        for (Row row : result)
        {
            for (String column : row.schema().fieldNames())
            {
                if (!column.equals(group) && App.valuesMap.get(row.getAs(group)) != null)
                {
                    dataset.addValue(row.getAs(column), App.valuesMap.get(row.getAs(group)), prova.get(column));
                }
            }
        }
        
        return dataset;
    }
    
    public JFreeChart mediaPorRendaFamiliar(int year)
    {
        String title = "Média da nota do ENEM por Renda Familiar";
        
        String[] columns = {"NU_NOTA_REDACAO", "NU_NOTA_MT", "NU_NOTA_LC", "NU_NOTA_CH", "NU_NOTA_CN"};
        
        CategoryDataset dataset = listRowToDataset(avg("Q006", columns, year), "Q006");
        
        // Cria o grafico
        JFreeChart chart = ChartFactory.createBarChart(title, "Renda", "Nota", dataset);
        
        createJFrame(chart, title);
        
        return chart;
    }
    
    public void createJFrame(JFreeChart chart, String title)
    {
        // Cria janela para exebicao do grafico
        ChartPanel panel = new ChartPanel(chart);
        
        JFrame frame = new JFrame(title);
        
        frame.add(panel);
        frame.pack();
        frame.setVisible(true);
    }
    
    public JFreeChart mediaProvaPorRendaFamiliar(String provaColumn, int year)
    {
        String title = "Média em "+ prova.get(provaColumn) + " por Renda Familiar";
        
        CategoryDataset dataset = listRowToDataset(avg("Q006", new String[] {provaColumn}, year), "Q006");
        
        JFreeChart chart = ChartFactory.createBarChart(title, "Renda", "Nota", dataset);
        
        createJFrame(chart, title);
        
        return chart;
    }
    
    public JFreeChart mediaPorProvaPorTipoEscola(String provaColumn, int year)
    {
        String title = "Média em "+ prova.get(provaColumn) + " por tipo de Escola";
        
        String groupBy = "TP_DEPENDENCIA_ADM_ESC";
        
        CategoryDataset dataset = defaultListRowToDataset(avg(groupBy, new String[] {provaColumn}, year), groupBy);
        
        JFreeChart chart = ChartFactory.createBarChart(title, "Renda", "Nota", dataset);
        
        createJFrame(chart, title);
        
        return chart;
    }
    
    public CategoryDataset defaultListRowToDataset(List<Row> result, String group)
    {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        // Transforma resultada da query para dataset dos graficos
        for (Row row : result)
        {
            for (String column : row.schema().fieldNames())
            {
                if (!column.equals(group) && row.getAs(group) != null)
                {
                    dataset.addValue(row.getAs(column), row.getAs(group), column);
                }
            }
        }
        
        return dataset;
    }
}
