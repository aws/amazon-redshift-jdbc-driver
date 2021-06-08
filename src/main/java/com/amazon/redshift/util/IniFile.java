package com.amazon.redshift.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IniFile {
	
	 private Pattern  SECTION_PATTERN  = Pattern.compile( "\\s*\\[([^]]*)\\]\\s*" );
   private Pattern  KEY_VAL_PATTERN = Pattern.compile( "\\s*([^=]*)=(.*)" );
   private Map<String, Map< String, String >>  sections  = new HashMap<>();

   public IniFile( String path ) throws IOException {
      load( path );
   }

   public void load( String path ) throws IOException {
  	 try( BufferedReader br = new BufferedReader( new FileReader( path ))) {
			String line;
			String section = null;
			while(( line = br.readLine()) != null ) {
			   Matcher m = SECTION_PATTERN.matcher( line );
			   if( m.matches()) {
			      section = m.group( 1 ).trim();
			   }
			   else if( section != null ) {
			      m = KEY_VAL_PATTERN.matcher( line );
			      if( m.matches()) {
			         String key   = m.group( 1 ).trim();
			         String value = m.group( 2 ).trim();
			         Map< String, String > kv = sections.get( section );
			         if( kv == null ) {
			            sections.put( section, kv = new HashMap<>());   
			         }
			         kv.put( key, value );
			      }
			   }
	     } // Loop
     }
  }
   
	 public Map< String, String > getAllKeyVals( String section ) {
	   Map< String, String > kv = sections.get( section );
	   return kv;
	}   
}
