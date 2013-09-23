package by.kslisenko.wordcount.hostprotocolcount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogAnalyseMapper extends Mapper<Object, Text, Text, Text> {

    /**
     * Returns the substring before the first occurrence of a delimiter. The
     * delimiter is not part of the result.
     *
     * @param string    String to get a substring from.
     * @param delimiter String to search for.
     * @return          Substring before the last occurrence of the delimiter.
     */
    public static String substringBeforeLast( String string, String delimiter )
    {
        int pos = string.lastIndexOf( delimiter );

        return pos >= 0 ? string.substring( 0, pos ) : string;
    }

    /**
     * Returns the substring after the first occurrence of a delimiter. The
     * delimiter is not part of the result.
     *
     * @param string    String to get a substring from.
     * @param delimiter String to search for.
     * @return          Substring after the last occurrence of the delimiter.
     */
    public static String substringAfterLast( String string, String delimiter )
    {
        int pos = string.lastIndexOf( delimiter );

        return pos >= 0 ? (pos + delimiter.length() < string.length() ?
                                string.substring( pos + delimiter.length() ) : "" ) : "";
    }


    public static void handleDirs( Context context, String filename ) throws IOException, InterruptedException
    {
        if (filename.length() < 1){
            // ignore empty filename
        } else if (filename.charAt(0) != '/') {
            System.out.println("invalid filename " + filename);
        } else if (filename.lastIndexOf("/") == 0) {
            context.write(new Text("dir"), new Text("/"));
            if (filename.length() > 1) {context.write(new Text("dir"), new Text(filename));}
        } else {
            context.write(new Text("dir"), new Text(filename));
            handleDirs(context, (substringBeforeLast(filename, "/")));
        }
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Split to strings
		String[] rows = value.toString().split("\n");
		
		// For each row get host as key and protocol as value
        // 1               2 3 4                     5      6    7                          8         9   10
		// uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0
		
		Pattern p = Pattern.compile("([\\S]*) [\\S]* [\\S]* [\\S]* [\\S]* [\\S]* ([\\S]*) [\\S]* [\\S]*");
		
		for (String logEntry : rows) {
			Matcher m = p.matcher(logEntry);
			if (m.find()) {
				String server = m.group(1);
				String targetfile = m.group(2);

				context.write(new Text(server), new Text(targetfile));
                context.write(new Text("server"), new Text(server));
                context.write(new Text("file"), new Text(targetfile));
                handleDirs(context, (substringBeforeLast(targetfile, "/")));
			}
		}
	}
}