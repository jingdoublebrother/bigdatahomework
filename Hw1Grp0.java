/*
 * File name: Hw1Grp0.java
 * Type: java
 * Author: liqing(liqingdoublebrother@gmail.com)
 * Created: 2016-04-01
 */

import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * A public class giving a user a tool working with HDFS and HBASE in the Pseudo-Distributed model.
 * With the class, a user can join 2 files with any chosen column in the files and write the result to HBASE.
 * In a terminal, after starting HDFS and HBASE, the tool can be started with the command:
 *     java Hw1Grp0 R=file_1 S=file_2 join:Rx=Sy res:Rm,...,Rn,Sm,...,Sn
 * For example: java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R10=S4 res:R0,R11,S0,S1
 * Then a table named 'Result' will be writen to HBASE.
 * In the table, a row key is the join column, only a column family named 'res' and  many chosen column keys.
 *
 * @author liqing(liqingdoublebrother@gmail.com)
 * @version 1.0
 * @since JDK1.7
 */
public class Hw1Grp0
{

    public static void main( String[] args ) throws Exception
    {
        if( args.length!= 4)
        {
            System.err.println("Usage: java Hw1Grp0 R=<file1> S=<file2> join:Rx=Sy res:Rm,...,Rn,Sm,...,Sn");
            System.exit(-1);
        }

        String file_R = args[0].substring(2);
        String file_S = args[1].substring(2);

        Pattern pattern_R = Pattern.compile( "R[0-9]+" );
        Matcher matcher = pattern_R.matcher(args[2]);
        matcher.find();
        int index_of_key_in_R = Integer.parseInt(matcher.group().substring(1));
        Pattern pattern_S = Pattern.compile( "S[0-9]+" );
        matcher = pattern_S.matcher( args[2] );
        matcher.find();
        int index_of_key_in_S = Integer.parseInt(matcher.group().substring(1));

        List<String> column_R_list = new ArrayList<String>();
        matcher = pattern_R.matcher( args[3] );
        while( matcher.find() )
            column_R_list.add( matcher.group() );
        List<String> column_S_list = new ArrayList<String>();
        matcher = pattern_S.matcher( args[3] );
        while( matcher.find() )
            column_S_list.add( matcher.group() );


        HashJoinWithHDFSAndHBASE my_join = new HashJoinWithHDFSAndHBASE( file_R, file_S,
                column_R_list, column_S_list, index_of_key_in_R, index_of_key_in_S );
        my_join.join();
    }

}

/**
 * The class HashJoinWithHDFSAndHBASE utilizing hashmap to join 2 files with the chosen join key, columns and writing the result to HBASE.
 * For example:
 *     HashJoinWithHDFSAndHBASE my_join = new HashJoinWithHDFSAndHBASE( file_R, file_S,
 *                                            columns_R, columns_S, index_of_key_in_R, index_of_key_in_S );
 *     my_join.join();
 *
 * @author liqing(liqingdoublebrother@gmail.com)
 * @version 1.0
 * @since JDK1.7
 */
class HashJoinWithHDFSAndHBASE
{
    private HashMap<String, List<String>> map_R;
    private HashMap<String, List<String>> map_S;

    private String file_R;
    private String file_S;

    private List<String> column_R_list;
    private List<String> column_S_list;

    private int index_of_key_in_R;
    private int index_of_key_in_S;

    /**
     * The constructor with params, initializing the hashmaps,file names,
     * the chosen columns and the index of the key column.
     *
     * @param file_R
     * The first file name in hdfs.
     * @param file_S
     * The second file name in hdfs.
     * @param column_R_list
     * The chosen columns in first file.
     * @param column_S_list
     * The chosen colums in second file.
     * @param index_of_key_in_R
     * The column index of the key column in first file.
     * @param index_of_key_in_S
     * The column index of the key column in second file.
     */
    public HashJoinWithHDFSAndHBASE( String file_R, String file_S,
            List<String> column_R_list, List<String> column_S_list, int index_of_key_in_R, int index_of_key_in_S  )
    {
        this.map_R = new HashMap<String, List<String>>();
        this.map_S = new HashMap<String, List<String>>();
        this.file_R =  file_R;
        this.file_S =  file_S;
        this.column_R_list = column_R_list;
        this.column_S_list = column_S_list;
        this.index_of_key_in_R = index_of_key_in_R;
        this.index_of_key_in_S = index_of_key_in_S;
    }

    /**
     * The function readHdfsFile can take a string file name, then return a BufferedReader
     * object used to read the file in hdfs.
     *
     * @param file
     * The file name in hdfs.
     * @return in
     * Retuen a BufferedReader.
     * @throws IOException
     * Throws the IOException.
     * @throws URISyntaxException.
     * Throws the URISyntaxException.
     */
    private BufferedReader readHdfsFile( String file ) throws IOException, URISyntaxException
    {
        file = "hdfs://localhost:9000/" + file;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        FSDataInputStream in_stream = fs.open( new Path( file ) );
        BufferedReader in = new BufferedReader( new InputStreamReader( in_stream ) );
        return in;
    }


    /**
     * The function createHBaseTable creates a table named 'Result',
     * and a columnFamily named 'res' in hbase.
     * Then returns a HTable object used to put record.
     *
     * @return table
     * A HTable object representing the table in hbase.
     * @throws MasterNotRunningException
     * Throws the MasterNotRunningException.
     * @throws ZooKeeperConnectionException
     * Throws the ZooKeeperConnectionException.
     * @throws IOException
     * Throws the IOException.
     *
     */
    private HTable createHBaseTable()
        throws MasterNotRunningException, ZooKeeperConnectionException, IOException
    {
        String table_name = "Result";
        HTableDescriptor htd = new HTableDescriptor( TableName.valueOf( table_name ));
        HColumnDescriptor hcd = new HColumnDescriptor( "res" );
        htd.addFamily(hcd);
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin h_admin = new HBaseAdmin( conf );

        if( h_admin.tableExists( table_name ) )
        {
            h_admin.disableTable( table_name );
            h_admin.deleteTable( table_name );
        }

        h_admin.createTable( htd );
        h_admin.close();

        HTable table = new HTable( conf, table_name );
        return table;
    }

    /**
     * The getColumnInLine function can get the string content of the chosen column.
     *
     * @param line
     * A string containing many columns.
     * @param index_of_column
     * The index of the column to be extracted.
     * @return matcher.group(1)
     * The string content of the chosen column.
     */
    private String getColumnInLine( String line, int index_of_column )
    {
        Pattern pattern = Pattern.compile( "([^\\|]*)(\\|)" );
        Matcher matcher = pattern.matcher( line );

        // int start_position = -1;
        // int end_position = -1;
        for( int i=0; i<= index_of_column; i++ )
        {
            // start_position = end_position + 1;
            // end_position = line.indexOf( '|', start_position);
            if( !matcher.find() )
            {
                System.err.println( "please check your input: the index of column is out of the range" );
                System.exit(-1);
            }
        }

        return matcher.group(1);
        // return line.substring( start_position, end_position );
    }


    /**
     * The function createHashTable creates a HashMap object, a key is the content of the key column in a file.
     * And the value is the liens with the same key in the file.
     * This function calls functions readHdfsFile, getColumnInLine to work.
     *
     * @param file
     * The name of a file to be converted to be a hashmap.
     * @param hash_table
     * The HashMap object containing the content of the file.
     * @param index_of_key_column
     * The column index of the key column.
     * @throws IOException
     * Throws the IOException.
     * @throws URISyntaxException
     * Throws the URISyntaxException.
     * @see #readHdfsFile( String file )
     * @see #getColumnInLine( String line, int index_of_column )
     */
    private void createHashTable( String file, HashMap<String, List<String>> hash_table, int index_of_key_column)
        throws IOException, URISyntaxException
    {
        BufferedReader in = readHdfsFile( file );
        String key;
        String line;
        while( (line=in.readLine() )!= null)
        {
            key = getColumnInLine( line, index_of_key_column );
            if( !hash_table.containsKey( key ) )
                {
                    List<String> bucket = new ArrayList<String>();
                    hash_table.put( key, bucket );
                }
            hash_table.get(key).add(line);
        }
    }


    /**
     * The join function joins 2 files in hdfs according the chosen columns,
     * and put the result to a table in hbase.
     *
     * @throws IOException
     * Throws the IOException.
     * @throws URISyntaxException
     * Throws the URISyntaxException.
     * @see #createHashTable
     * @see #getColumnInLine
     */
    public  void join()throws IOException, URISyntaxException
    {
        createHashTable( file_R, map_R, index_of_key_in_R );
        createHashTable( file_S, map_S, index_of_key_in_S );

        HTable h_table = createHBaseTable();

        /* joinCount records the number of the join result with the same key */
        HashMap<String, Integer> joinCount = new HashMap<String, Integer>();

        /* for every key in the second hashmap, to check whether the first hashmap contains.
         * if true, then check every line in the value of the key in the second hashmap */
        for( String key:map_S.keySet() )
        {
            if( map_R.containsKey( key ) )
            {
                for( String line:map_S.get( key ) )
                {
                    if( !joinCount.containsKey( key ) )
                      joinCount.put( key, 0 );

                    Put put = new Put( key.getBytes() );
                    for( String record:map_R.get( key ) )
                    {
                      int count = joinCount.get( key );

                      for( String column:column_R_list  )
                      {
                          int index_of_column_in_R = Integer.parseInt( column.substring(1) );
                          String value = getColumnInLine( record,  index_of_column_in_R);
                          if( count > 0 )
                            column = column + "." + String.valueOf( joinCount.get( key ) );
                          put.add( "res".getBytes(), column.getBytes(), value.getBytes() );
                      }

                      for( String column:column_S_list )
                      {
                           int index_of_column_in_S = Integer.parseInt( column.substring(1) );
                           String value = getColumnInLine( line, index_of_column_in_S );
                           if( count>0 )
                             column = column + "." + String.valueOf( joinCount.get( key ) );
                           put.add( "res".getBytes(), column.getBytes(), value.getBytes() );
                      }

                      count++;
                      joinCount.put( key, count );
                    }

                    h_table.put(put);
                }
            }
        }

        h_table.close();
    }
}
