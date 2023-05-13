package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartUtils;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Podcast Analysis")
                .master("local[*]")
                .getOrCreate();

        String categoryPath = "input/categories.json";
        Dataset<Row> categoryDF = spark.read().json(categoryPath);

        String reviewPath = "input/reviews.json";
        Dataset<Row> reviewDF = spark.read().json(reviewPath);

        Dataset<Row> joinedDF = reviewDF.join(categoryDF, "podcast_id");

        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // group by category ID and calculate average rating and count of records
        Dataset<Row> aggDF = joinedDF.groupBy("podcast_id")
                .agg(functions.avg("rating").alias("avg_rating"), functions.count("*").alias("record_count"))
                .orderBy(functions.desc("record_count"));

        Dataset<Row> aggJoinedDF = aggDF.join(categoryDF, "podcast_id");

        // 1. Get the podcast category with the highest average rating
        List<Row> finalDF = aggJoinedDF
                .limit(20)
                .collectAsList();

        for (Row row : finalDF) {
            System.out.println(row);
            String category = row.getString(3);
            Double avgRating = row.getDouble(1);
            dataset.addValue(avgRating, "Average Rating", category);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Average Rating by Podcast category",
                "Podcast Category",
                "Average Rating",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        try {
            System.out.println("Trying to create file");
            File newFile = new File("output/review_counts.jpeg");
            ChartUtils.saveChartAsJPEG(newFile, chart, 4000, 1000);
        } catch (IOException e) {
            System.out.println("An error occurred.");
        }

        // 2nd count reviews by year
        Dataset<Row> yearDF = reviewDF.withColumn("year", functions.year(functions.to_date(functions.col("created_at"))));
        Dataset<Row> joinedForYear = yearDF.join(categoryDF, "podcast_id")
                .groupBy("year", "category")
                .count();

        // get sum of count by category
        Dataset<Row> sumDF = joinedForYear.groupBy("category")
                .agg(functions.sum("count").alias("total_count"))
                .orderBy(functions.desc("total_count"));

        // get top 10 category name and assign them to a list
        List<Row> topCategoryDF = sumDF.limit(10).collectAsList();

        List<String> topCategories = new ArrayList<String>();

        for (Row row: topCategoryDF) {
            String categoryName = row.getString(0);
            topCategories.add(categoryName);
        }

        // filter joinedForYear by top 10 categories
        // and then group by year and category
        Dataset<Row> filteredDF = joinedForYear
                .filter(functions.col("category").isin(topCategories.toArray()))
                .orderBy(functions.asc("year"), functions.asc("category"));

        // iterate through each category and create a line chart throughout the years
        DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();

        for (String category: topCategories) {
            List<Row> categoryRows = filteredDF
                    .filter(functions.col("category").equalTo(category))
                    .collectAsList();

            for (Row row: categoryRows) {
                Integer year = row.getInt(0);
                Long count = row.getLong(2);
                categoryDataset.addValue(count, category, year);
            }
        }

        JFreeChart categoryChart = ChartFactory.createBarChart(
                "Review Count by Year",
                "Year",
                "Review Count",
                categoryDataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        try {
            System.out.println("Trying to create file");
            File newFile = new File("output/yearly_review_count_top_10_categories.jpeg");
            ChartUtils.saveChartAsJPEG(newFile, categoryChart, 4000, 1000);
        } catch (IOException e) {
            System.out.println("An error occurred.");
        }


        spark.stop();
    }
}