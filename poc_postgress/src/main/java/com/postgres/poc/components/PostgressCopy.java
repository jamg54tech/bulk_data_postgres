package com.postgres.poc.components;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
@Component
public class PostgressCopy {

    //private static Logger logger= LogManager.getLogger(PostgressCopy.class);

    @Value("${postgres.url}")
    private String url;
    @Value("${postgres.user}")
    private String user;
    @Value("${postgres.password}")
    private String password;


    public void loadData(String tableName,String filePath, String [] fields){

        try  {
            System.out.println("URL:"+this.url);
            System.out.println("USER:"+this.user);
            System.out.println("PASSWORD:"+this.password);
            System.out.println("PATH:"+filePath);

            Connection conn = DriverManager.getConnection(this.url, this.user, this.password);

            CopyManager cp = new CopyManager((BaseConnection) conn);

            String command="COPY "+tableName +" (" + String.join(", ", fields) + ") FROM STDIN WITH CSV DELIMITER '|'";

            System.out.println("COMMAND:"+command);

            long rowsInserted= cp.copyIn(
                    command,
                    new BufferedReader(new FileReader(filePath))
            );
            System.out.printf("%d row(s) inserted%n", rowsInserted);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void loadSKUS(String tableName,String filePath) throws IOException {

        CsvReadOptions options = CsvReadOptions.builder(filePath)
                .separator('|')
                .header(false) // Indica si el archivo CSV tiene un encabezado
                .build();



        try  {

            // Read the CSV file with the specified options
            Table table = Table.read().csv(options);
            StringColumn column1 = (StringColumn) table.column(1);

            // Get the current date and time
            LocalDateTime currentDateTime = LocalDateTime.now();
            // Define a custom format pattern
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            // Format the current date and time using the custom pattern
            String formattedDateTime = currentDateTime.format(formatter);

            //column1.set();

            System.out.println("URL:"+this.url);
            System.out.println("USER:"+this.user);
            System.out.println("PASSWORD:"+this.password);
            System.out.println("PATH:"+filePath);

            Connection conn = DriverManager.getConnection(this.url, this.user, this.password);

            CopyManager cp = new CopyManager((BaseConnection) conn);

            String command="COPY "+tableName+" FROM STDIN WITH CSV DELIMITER '|'";

            System.out.println("COMMAND:"+command);

            long rowsInserted= cp.copyIn(
                    command,
                    new BufferedReader(new FileReader(filePath))
            );
            System.out.printf("%d row(s) inserted%n", rowsInserted);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void loadPromotionRelations(String tableName,String filePath) throws IOException {

        try  {
            System.out.println("URL:"+this.url);
            System.out.println("USER:"+this.user);
            System.out.println("PASSWORD:"+this.password);
            System.out.println("PATH:"+filePath);

            Connection conn = DriverManager.getConnection(this.url, this.user, this.password);

            CopyManager cp = new CopyManager((BaseConnection) conn);

            String command="COPY "+tableName+" FROM STDIN WITH CSV DELIMITER '|'";

            System.out.println("COMMAND:"+command);

            long rowsInserted= cp.copyIn(
                    command,
                    new BufferedReader(new FileReader(filePath))
                    );
            System.out.printf("%d row(s) inserted%n", rowsInserted);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





}
