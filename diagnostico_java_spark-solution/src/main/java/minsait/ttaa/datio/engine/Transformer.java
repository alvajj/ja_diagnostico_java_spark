package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.naming.Params;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static minsait.ttaa.datio.common.Common.HEADER;
import static minsait.ttaa.datio.common.Common.INFER_SCHEMA;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.when;

public class Transformer extends Writer {
    private final SparkSession spark;
    Params params = new Params();

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;

        InputStream inputStream;
        Path pt = new Path("src/test/resources/params");
        FileSystem fs;
        Properties properties = new Properties();
        {
            try {
                fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
                inputStream = fs.open(pt);
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        params.setOutputFile((String) properties.get("spark.app.output"));
        params.setNameCsv((String) properties.get("spark.app.namecsv"));
        System.out.println(properties.get("spark.app.parameter"));
        String parameterString = (String) properties.get("spark.app.parameter");
        params.setParameter(Integer.parseInt(parameterString.trim()));

        Dataset<Row> df = readInput();
        if (params.getParameter() == 1) {
            df = windowFilterAge(df);//Se aplica el filtro de la edad
            df = windowPalyerCat(df);
            //df = windowFilter(df); //Si player_cat esta en los siguientes valores: A, B
            //df = windowFilterCat2(df); //Si player_cat es C y potential_vs_overall es superior a 1.15
            df = windowFilterCat3(df); //Si player_cat es D y potential_vs_overall es superior a 1.25

            df.show(100); //Mostrando las dos columnas agregadas
            //df = columnSelection(df);//mostrando solo las columnas establecidas en el punto 1 del ejercicio
            //df.show(100);


        } else if (params.getParameter() == 0) {
            df = windowPalyerCat(df);
            df = windowFilter(df);
            //df = windowFilterCat2(df); //Si player_cat es C y potential_vs_overall es superior a 1.15
            //df = windowFilterCat3(df); //Si player_cat es D y potential_vs_overall es superior a 1.25

            df.show(100); //Mostrando las dos columnas agregadas
            df = columnSelection(df);//mostrando solo las columnas establecidas en el punto 1 del ejercicio
            df.show(100);
        } else {
            showAll(df);
        }
        df.coalesce(1).write().mode("append").partitionBy("nationality").parquet(params.getOutputFile());

    }

    private Dataset<Row> showAll(Dataset<Row> df) {
        df = cleanData(df);
        df = columnSelection(df);
        df.show(Integer.MAX_VALUE, false);
        return df;
    }


    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                long_name.column(),
                age.column(),
                heightCm.column(),
                weight_kg.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column()
        );
    }
    private Dataset<Row> showPlayerCat(Dataset<Row> df) {
        df = cleanData(df);
        df = windowPalyerCat(df);
        df = columnSelectionCat(df);
        df.show(100, true);
        return df;
    }

    private Dataset<Row> columnSelectionCat(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                long_name.column(),
                age.column(),
                heightCm.column(),
                weight_kg.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column()
        );
    }

    private Dataset<Row> columnSelectionCatOv(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                long_name.column(),
                age.column(),
                heightCm.column(),
                weight_kg.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column(),
                potentialVSoverall.column()
        );
    }



    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(params.getNameCsv());
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> windowPalyerCat(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(3), "A")
                .when(rank.$less(5), "B")
                .when(rank.$less(10), "C")
                .otherwise("D");

        df = df.withColumn(playerCat.getName(), rule);
        df = df.withColumn(potentialVSoverall.getName(), potential.column().divide(overall.column()));
        df = columnSelectionCatOv(df);
        return df;
    }

    private Dataset<Row> windowFilter(Dataset<Row> df) {
        df = df.filter(playerCat.column().equalTo("A").or(playerCat.column().equalTo("B")));
        return df;
    }

    private Dataset<Row> windowFilterCat2(Dataset<Row> df) {
        df = df.filter(potentialVSoverall.getName() + " > 1.15").filter(playerCat.column().equalTo("C"));
        return df;
    }

    private Dataset<Row> windowFilterCat3(Dataset<Row> df) {
        df = df.filter(potentialVSoverall.getName() + " > 1.15").filter(playerCat.column().equalTo("D"));
        return df;
    }

    private Dataset<Row> windowFilterAge(Dataset<Row> df) {
        df = df.filter(age.column().$less("23"));
        return df;
    }
}