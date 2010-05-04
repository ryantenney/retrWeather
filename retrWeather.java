/*
 * retrWeather
 *
 * Ryan Tenney 2007
 *
 * Download weather data from weather.gov and save to a sql server
 * 
 * released under gpl v2
 *
 */

import java.io.*;
import java.sql.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.macfaq.net.URLGrabber;

public class retrWeather
{
	// Debug "macro" to turn off the execution of SQL statements.
	final static boolean SQL_ACTIVE = true;
	
	// Regular expressions for parsing XML elements, and date strings (as per RFC 822)
	final static String regex_xmlElement = "<([a-zA-Z0-9_]+)>(.+?)</\\1>";
	final static String regex_rfc2822date = "([a-zA-Z]{3}\\s*,\\s+)?([0-3]?[0-9])\\s+([a-zA-Z]{3})\\s+([0-9]{2,4})\\s+([0-2][0-9]"
										  + ":[0-5][0-9](:[0-5][0-9])?)(\\s+([\\-+][0-2][0-9][0-5][0-9])?([a-zA-Z]{1,3})?){1,2}";

	private String[] database_columns;
	private String[] station_ids;
	private String[] station_fields;
	
	private Connection connection;
	
	/*
	 * Constructor
	 */
	public retrWeather()
	{
		boolean initResult = readConfig();
		
		if( !initResult )
		{
			System.out.println( "ReadConfig() failed." );
			System.exit( 0 );
		}
		
		database_columns = splitTrimCsv( config_keys.get( "database_columns" ) );
		station_ids = splitTrimCsv( config_keys.get( "station_ids" ) );
		station_fields = splitTrimCsv( config_keys.get( "station_fields" ) );

		try
		{
			Class.forName( _config( "connector_classname" ) ).newInstance();
		}
		catch( Exception ex )
		{
			ex.printStackTrace();
		}

		try
		{
			connection = DriverManager.getConnection( _config( "connection_string" ), _config( "username" ), _config( "password" ) );
		}
		catch( SQLException sqlex )
		{
			System.err.println("SQLException: " + sqlex.getMessage()); 
			System.err.println("SQLState: " + sqlex.getSQLState()); 
			System.err.println("VendorError: " + sqlex.getErrorCode());
		}
	}
	
	
	/*
	 * Configuration
	 */
	
	// Config filename to look for in the classpath directory
	final static String configFileName = "retrWeather.xml";
	
	// 
	private Map< String, String > config_keys;
	
	//
	public String _config( String key )
	{
		return config_keys.get( key );
	}
	
	// Shortcut to retrieve config name/value pairs, and replace $i with params[ i ]
	public String _config( String key, String[] params )
	{
		String ret = config_keys.get( key );
		
		for( int i = 0; i < params.length; i++ )
		{
			ret = ret.replaceAll( "\\$" + Integer.toString( i ), params[ i ] );
		}
		
		return ret;
	}
	
	// Read in xml configuration file
	public boolean readConfig()
	{
		try
		{
			File configFile = new File( System.getProperty( "java.class.path" ), configFileName );
			BufferedReader in = new BufferedReader( new FileReader( configFile ) );
			StringBuffer sb = new StringBuffer( new Long( configFile.length() ).intValue() );

			char[] buf = new char[ 1024 ];
			int len = 0;
			
			while( ( len = in.read( buf ) ) > 0 )
			{
				sb.append( new String( buf, 0, len ) );
			}
			
			config_keys = parseXmlNameValuePairs( sb.toString(), true );

			return true;
		}
		catch( FileNotFoundException fnfex )
		{
			fnfex.printStackTrace();
			return false;
		}
		catch( IOException ioex )
		{
			ioex.printStackTrace();
			return false;
		}
	}
	
	
	/*
	 * Character escaping
	 */
	
	// Mapping of reserved chars to the escape code
	final static String[][] escape = new String[][] { { "&amp;" , "&"  },
													  { "&quot;", "\"" },
													  { "&apos;", "'"  },
													  { "&lt;"  , "<"  },
													  { "&gt;"  , ">"  } };
	
 	// Escapes reserved chars to make safe for an XML/HTML string ( '&' -> '&amp;' )
	public static String escapeChars( String str )
	{
		for( String[] pair : escape )
		{
			str = str.replaceAll( pair[ 1 ], pair[ 0 ] );
		}
		return str;
	}
	
	// Unescapes reserved chars in an XML/HTML string ( '&amp;' -> '&' )
	public static String unescapeChars( String str )
	{
		for( String[] pair : escape )
		{
			str = str.replaceAll( pair[ 0 ], pair[ 1 ] );
		}
		return str;
	}
	
	
	/*
	 * Date interpretation
	 */

	//
	public static int getMonthByName( String monthName )
	{
		int ret = -1;
		
		int[] i = new int[] { (int)monthName.charAt( 0 ),
							  (int)monthName.charAt( 1 ),
							  (int)monthName.charAt( 2 )
		};
		
		char[] c = new char[] { (char)( i[ 0 ] >= 97 ? i[ 0 ] - 32 : i[ 0 ] ),
								(char)( i[ 1 ] >= 97 ? i[ 1 ] - 32 : i[ 1 ] ),
								(char)( i[ 2 ] >= 97 ? i[ 2 ] - 32 : i[ 2 ] )
		};
		
		if( c[ 0 ] == 'A' )
		{
			if( c[ 1 ] == 'P' && c[ 2 ] == 'R' )
				ret = 3;					// April
			else if( c[ 1 ] == 'U' && c[ 2 ] == 'G' )
				ret = 7;					// August
		}
		else if( c[ 0 ] == 'D' )
		{
			if( c[ 1 ] == 'E' && c[ 2 ] == 'C' )
				ret = 11;
		}
		else if( c[ 0 ] == 'F' )
		{
			if( c[ 1 ] == 'E' && c[ 2 ] == 'B' )
				ret = 1;					// February
		}
		else if( c[ 0 ] == 'J' )
		{
			if( c[ 1 ] == 'A' )
			{
				if( c[ 2 ] == 'N' )
					ret = 0;			// January
			}
			else if( c[ 1 ] == 'U' )
			{
				if( c[ 2 ] == 'N' )
					ret = 5;		// June
				else if( c[ 2 ] == 'L' )
					ret = 6;
			}
		}
		else if( c[ 0 ] == 'M' )
		{
			if( c[ 1 ] == 'A' )
			{
				if( c[ 2 ] == 'R' )
					ret = 2;			// March
				else if( c[ 2 ] == 'Y' )
					ret = 4;			// May
			}
		}
		else if( c[ 0 ] == 'N' )
		{
			if( c[ 1 ] == 'O' && c[ 2 ] == 'V' )
				ret = 10;					// November
		}
		else if( c[ 0 ] == 'O' )
		{
			if( c[ 1 ] == 'C' && c[ 2 ] == 'T' )
				ret = 9;
		}
		else if( c[ 0 ] == 'S' )
		{
			if( c[ 1 ] == 'E' && c[ 2 ] == 'P' )
				ret = 8;
		}
	
		return ret;
	}
	
	//
	public static Calendar parseRfc822Date( String rfc822str, boolean convertToUTC )
	{
		Matcher m = Pattern.compile( regex_rfc2822date ).matcher( rfc822str );

		if( m.find() )
		{
			int day, month, year, hour, mins, secs;
			
			day = Integer.parseInt( m.group( 2 ) );
			month = getMonthByName( m.group( 3 ) );
			year = Integer.parseInt( m.group( 4 ) );
			if( m.group( 4 ).length() == 2 )
				year = year < 70 ? year + 2000 : year + 1900;
			
			String[] timeParts = m.group( 5 ).split( ":", 3 );
			hour = Integer.parseInt( timeParts[ 0 ] );
			mins = Integer.parseInt( timeParts[ 1 ] );
			secs = timeParts.length == 2 ? Integer.parseInt( timeParts[ 2 ] ) : 0;
			
			TimeZone tz = TimeZone.getTimeZone( m.group( 8 ).length() > 0 ? ( "GMT" + m.group( 8 ) ) : m.group( 9 ) );
			Calendar cal = new GregorianCalendar();

			if( convertToUTC )
				cal.setTimeZone( tz );

			cal.set( year, month, day, hour, mins, secs );
			
			cal.set( Calendar.MILLISECOND, 0 );

			if( convertToUTC )
			{
				Calendar utc = new GregorianCalendar( TimeZone.getTimeZone( "GMT" ) );
				utc.setTimeInMillis( cal.getTimeInMillis() );
				cal = utc;
			}
			
			return cal;
		}
		else
		{
			return null;
		}
	}
	
	//
	public static String calendarToDateString( Calendar cal )
	{
		StringBuilder sb = new StringBuilder( 20 );
		
		sb.append( cal.get( Calendar.YEAR ) );
		sb.append( "/" );
		sb.append( pad0( cal.get( Calendar.MONTH ) + 1, 2 ) );
		sb.append( "/" );
		sb.append( pad0( cal.get( Calendar.DAY_OF_MONTH ), 2 ) );
		
		sb.append( " " );
		
		sb.append( pad0( cal.get( Calendar.HOUR_OF_DAY ), 2 ) );
		sb.append( ":" );
		sb.append( pad0( cal.get( Calendar.MINUTE ), 2 ) );
		sb.append( ":" );
		sb.append( pad0( cal.get( Calendar.SECOND ), 2 ) );
		
		return sb.toString();
	}
	
	//
	public static Timestamp getTimestamp( String rfc822str, boolean convertToUTC )
	{
		return new Timestamp( parseRfc822Date( rfc822str, convertToUTC ).getTimeInMillis() );
	}
	
	//
	public static String pad0( int val, int length )
	{
		StringBuffer ret;
		
		ret = new StringBuffer( length );
		ret.append( val );
		while( length > ret.length() )
			ret.insert( 0, '0' );
		
		return ret.toString();
	}
	
	
	/*
	 * SQL operations
	 */
	
	//
	public String constructObservationInsert( Map< String, String > entries )
	{
		// add field 'observation_time'
		Calendar cal = parseRfc822Date( entries.get( _config( "rfc822_date_string" ) ), new Boolean( _config( "datetime_utc" ) ) );
		entries.put( _config( "datetime_value" ), calendarToDateString( cal ) );
		entries.put( "observation_time_posix", Long.toString( cal.getTimeInMillis() ) );
		
		//entries.put( "hash", "UNHEX('" + toHex( hashRecord( entries.get( "station_id" ), cal.getTimeInMillis() ) ) + "')" );
		//entries.put( "hash", new String( hashRecord( entries.get( "station_id" ), cal.getTimeInMillis() ) ) );
			
		return constructInsert( "`weather`.`observed`", database_columns, entries );
	}
	
	//
	public String constructInsert( String table, String[] fields, Map< String, String > data )
	{
		StringBuilder sqlNames = new StringBuilder();
		StringBuilder sqlValues = new StringBuilder();
		
		for( String field : fields )
		{
			if( data.containsKey( field ) )
			{
				if( sqlNames.length() != 0 && sqlValues.length() != 0 )
				{
					sqlNames.append( " , " );
					sqlValues.append( " , " );
				}
				
				sqlNames.append( _config( "sqlFieldQuoteChar" ) );
				sqlNames.append( field );
				sqlNames.append( _config( "sqlFieldQuoteChar" ) );
				
				if( data.get( field ).equals( "NA" ) )
				{
					sqlValues.append( "NULL" );
				}
				else
				{
					sqlValues.append( _config( "sqlValueQuoteChar" ) );
					sqlValues.append( data.get( field ) );
					sqlValues.append( _config( "sqlValueQuoteChar" ) );
				}
			}
		}
		
		sqlNames.insert( 0, "INSERT INTO " + table + " (" );
		sqlNames.append( " ) VALUES ( " );
		sqlNames.append( sqlValues );
		sqlNames.append( " );" );
		
		return sqlNames.toString();
	}
	
	//
	public Calendar getLastRecord( String station_id )
	{
		try
		{
			CallableStatement call = connection.prepareCall( "CALL `weather`.`lastRecord`( ?, ? );" );
			
			call.registerOutParameter( 2, Types.VARCHAR );
			call.setString( 1, station_id );
			call.execute();

			if( call.getString( 2 ) != null )
				return parseRfc822Date( call.getString( 2 ), new Boolean( _config( "datetime_utc" ) ) );
			else
				return null;
		} catch( SQLException sqlex ) {
			System.err.println("SQLException: " + sqlex.getMessage()); 
			System.err.println("SQLState: " + sqlex.getSQLState()); 
			System.err.println("VendorError: " + sqlex.getErrorCode());
			return null;
		}
	}

	
	/*
	 * XML parsing
	 */
	
	//
	public static String[] splitTrimCsv( String value )
	{
		value = value.replaceAll( "[\\s\\r\\n]+", "" );
		String[] ret = value.split( "," );
		
		for( String element : ret )
		{
			element = element.trim();
		}
		
		return ret;
	}
	
	//
	public static Map< String, String > parseXmlNameValuePairs( String xml, boolean dot_all )
	{
		Map< String, String > pairs = new Hashtable< String, String >();
		Matcher m = Pattern.compile( regex_xmlElement, dot_all ? Pattern.DOTALL : 0 ).matcher( xml );
		
		while( m.find() )
		{
			pairs.put( m.group( 1 ), m.group( 2 ) );
		}
		
		return pairs;
	}
	
	//
	public static Set< String > parseXmlElements( String xml, boolean dot_all )
	{
		Set< String > element = new HashSet< String >();
		Matcher m = Pattern.compile( regex_xmlElement, dot_all ? Pattern.DOTALL : 0 ).matcher( xml );
		
		while( m.find() )
		{
			element.add( m.group( 0 ) );
		}
		
		return element;
	}
	
	//
	public static Set< String > parseXmlElementsByName( String xml, String element )
	{
		Set< String > elements = new HashSet< String >();
		Matcher m = Pattern.compile( regex_xmlElement, Pattern.DOTALL ).matcher( xml );
		
		while( m.find() ) {
			if( element.equalsIgnoreCase( m.group( 1 ) ) ) {
				elements.add( m.group( 2 ) );
			}
		}
		
		return elements;
	}	
	
	
	/*
	 * Digest functions
	 */
	
	//
	public static byte[] hashRecord( String station_id, long timeInMillis )
	{
		try
		{
			MessageDigest dig = MessageDigest.getInstance( "MD5" );
			
			dig.update( station_id.getBytes() );
			for( int i = 0; i < 7; i++ )
			{
				dig.update( (byte)( ( timeInMillis >> ( i * 8 ) ) & 0xFF ) );
			}
			
			return dig.digest();
		}
		catch( NoSuchAlgorithmException nsaex )
		{
			return null;
		}
	}
	
	//
	public static String toHex( byte[] data )
	{
		char[] HEX = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
		StringBuffer sb = new StringBuffer( data.length * 2 );
		for( byte b : data )
		{
			sb.append( HEX[ ( b & 0xF0 ) >> 4 ] );
			sb.append( HEX[ b & 0x0F ] );
		}
		return sb.toString();
	}
	
	
	/*
	 * URL
	 */
	
	// retrieves current weather data from NOAA given a station id
	public String getStationData( String station_id ) throws MalformedURLException, IOException
	{
		return URLGrabber.getDocumentAsString( _config( "station_data_url", new String[] { station_id } ) );
	}
	
	// retrieves a list of all weather stations from NOAA
	public String getStationIndex() throws MalformedURLException, IOException
	{
		return URLGrabber.getDocumentAsString( _config( "station_index_url" ) );
	}
	
	
	/*
	 * Database functions
	 */
	
	// Retrieve and store data for the station ids specified in the config file
	public void retrieveAndStore()
	{
		retrieveAndStore( station_ids );
	}
	
	// Retrieve and store data for the station ids passed to this function
	public void retrieveAndStore( String[] ids )
	{
		try
		{
			Statement s = connection.createStatement();
			
			for( String id : ids )
			{
				try
				{
					String xml = getStationData( id );
					Map< String, String > pairs = parseXmlNameValuePairs( xml, false );
					
					Calendar last = getLastRecord( id );
					Calendar next = parseRfc822Date( pairs.get( "observation_time_rfc822" ), new Boolean( _config( "datetime_utc" ) ) );

					if( !SQL_ACTIVE ) {
						System.out.println( "\nStation : " + id );
						
						if( last != null ) {
							System.out.println( "Compare : " + next.compareTo( last ) );
						}

						System.out.println( "Last    : " + ( last != null ? calendarToDateString( last  ) : "never" ) );
						System.out.println( "Next    : " + calendarToDateString( next ) );
					}
					
					if( last == null || next.compareTo( last ) == 1 ) {
						String query = constructObservationInsert( pairs );						
						s.addBatch( query );
						if( !SQL_ACTIVE ) {
							System.out.println( "Query   : " + query );
						}
					}
				}
				catch( Exception ex )
				{
					System.err.println( "Error on Station ID: " + id );
					ex.printStackTrace();
				}
			}
			
			if( SQL_ACTIVE ) {
				s.executeBatch();
			}
			
			s.close();
		}
		catch( SQLException sqlex )
		{
			System.err.println("SQLException: " + sqlex.getMessage()); 
			System.err.println("SQLState: " + sqlex.getSQLState()); 
			System.err.println("VendorError: " + sqlex.getErrorCode());
		}
	}
	
	// Retrieve and store station data
	public void retrieveStationData()
	{
		try {
			Statement s = connection.createStatement();
			
			Map< String, String > xml = parseXmlNameValuePairs( getStationIndex(), true );
			Set< String > stations = parseXmlElementsByName( xml.get( "wx_station_index" ), "station" );
			
			for( String station : stations ) {
				Map< String, String > values = parseXmlNameValuePairs( station, false );
				String query = constructInsert( "`weather`.`stations`", station_fields, values );
				if( SQL_ACTIVE ) {
					s.addBatch( query );
				} else {
					System.out.println( query + "\n" );
				}					
			}
			if( SQL_ACTIVE ) s.executeBatch();
		} catch( MalformedURLException muex ) {
			muex.printStackTrace();			
		} catch( IOException ioex ) {
			ioex.printStackTrace();
		} catch( SQLException sqlex ) {
			System.err.println("SQLException: " + sqlex.getMessage()); 
			System.err.println("SQLState: " + sqlex.getSQLState()); 
			System.err.println("VendorError: " + sqlex.getErrorCode());
		} catch( Exception ex ) {
			ex.printStackTrace();
		}
	}
	
	// Retrieve and store weather data for all stations
	public void retrieveForAllStations()
	{
		try {
			Statement s = connection.createStatement();
			
			String query = "SELECT `station_id` FROM `weather`.`stations`;";
			ResultSet rs = s.executeQuery( query );
			Vector< String > ids = new Vector< String >();
			
			while( rs.next() ) {
				ids.add( rs.getString( "station_id" ) );
			}
			
			s.close();

			String[] array_ids = new String[ ids.size() ];
			ids.toArray( array_ids );
			
			retrieveAndStore( array_ids );

		} catch( SQLException sqlex ) {
			System.err.println("SQLException: " + sqlex.getMessage()); 
			System.err.println("SQLState: " + sqlex.getSQLState()); 
			System.err.println("VendorError: " + sqlex.getErrorCode());
		}
	}
	
	// Iterates through String values stored in field `observation_time_rfc822`, reparses and stores TimeStamp value to field `observation_time`
	public void reparseStoredDates()
	{
		try
		{
			Statement s = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
			ResultSet q = s.executeQuery( "SELECT * FROM `weather`.`observed`" );

			while( q.next() )
			{
				//q.updateString( "observation_time", calendarToDateString( parseRfc822Date( q.getString( "observation_time_rfc822" ), new Boolean( _config( "datetime_utc" ) ) ) ) );
				long t = parseRfc822Date( q.getString( "observation_time_rfc822" ), new Boolean( _config( "datetime_utc" ) ) ).getTimeInMillis();
				q.updateTimestamp( "observation_time", new Timestamp( t ) );
				q.updateLong( "observation_time_posix", t );
				q.updateRow();
			}
			
			q.close();
			s.close();
		}
		catch( SQLException sqlex )
		{
			System.err.println("SQLException: " + sqlex.getMessage());
			System.err.println("SQLState: " + sqlex.getSQLState());
			System.err.println("VendorError: " + sqlex.getErrorCode());
		}
	}
	
	// Iterates through all records and generates a hash for each record of the station id and the posix time of the observation
	public void rehash()
	{
		try
		{
			Statement s = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
			ResultSet q = s.executeQuery( "SELECT * FROM `weather`.`observed`" );
			
			while( q.next() )
			{
				//long time = parseRfc822Date( q.getString( "observation_time_rfc822" ), new Boolean( _config( "datetime_utc" ) ) ).getTimeInMillis();
				long time = q.getLong( "observation_time_posix" );
				byte[] md5 = hashRecord( q.getString( "station_id" ), time );
				System.out.println( q.getString( "station_id" ) + ", " + q.getString( "observation_time_rfc822" ).replace( ',', '.' ) + ", " + time + ", " + toHex( md5 ) );
				q.updateBytes( "hash", md5 );
				q.updateRow();
			}
			
			s.close();
			q.close();
		}
		catch( SQLException sqlex )
		{
			System.err.println("SQLException: " + sqlex.getMessage());
			System.err.println("SQLState: " + sqlex.getSQLState());
			System.err.println("VendorError: " + sqlex.getErrorCode());
		}
	}
	
	
	
	/*
	 * Main
	 */
	public static void main( String[] args )
	{
		retrWeather r = new retrWeather();
		
		if( args.length >= 1 && args[ 0 ].equalsIgnoreCase( "up_date" ) ) {
			r.reparseStoredDates();
		} else if( args.length >= 1 && args[ 0 ].equalsIgnoreCase( "rehash" ) ) {
			r.rehash();
		} else if( args.length >= 1 && args[ 0 ].equalsIgnoreCase( "station_list" ) ) {
			r.retrieveStationData();
		} else if( args.length >= 1 && args[ 0 ].equalsIgnoreCase( "all_stations" ) ) {
			r.retrieveForAllStations();
		} else if( args.length == 0 ) {
			r.retrieveAndStore();
		} else if( args.length >= 1 ) {
			r.retrieveAndStore( args );
		}
		
		
	}

}