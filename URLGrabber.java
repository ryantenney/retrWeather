/*
 * URLGrabber.java
 * com.macfaq.net.URLGrabber
 * http://www.cafeconleche.org/books/xmljava/chapters/ch02s02.html
 * Assumed Copyright 2001, 2002
 * Elliotte Rusty Harold
 * elharo@metalab.unc.edu
*/

package com.macfaq.net;

import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;

public class URLGrabber
{
	
	public static InputStream getDocumentAsInputStream( URL url ) throws IOException
	{
		InputStream in = url.openStream();
		return in;
	}
	
	public static InputStream getDocumentAsInputStream( String url ) throws MalformedURLException, IOException
	{
		URL u = new URL( url );
		return getDocumentAsInputStream( u );
	}
	
	public static String getDocumentAsString( URL url ) throws IOException
	{
		StringBuffer result = new StringBuffer();
		InputStream in = url.openStream();
		int c;
		while ( ( c = in.read() ) != -1 ) result.append( (char) c );
		return result.toString();
	}
	
	public static String getDocumentAsString( String url ) throws MalformedURLException, IOException
	{
		URL u = new URL( url );
		return getDocumentAsString( u );
	}
	
}
