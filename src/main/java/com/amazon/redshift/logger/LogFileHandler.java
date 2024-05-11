package com.amazon.redshift.logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.redshift.jdbc.ResourceLock;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

public class LogFileHandler implements LogHandler {

  private static final int FILE_SIZE = 10 * 1024 * 1024; // 10 MB
	
  private static final int FILE_COUNT = 10;
  
  private static final String FILE_EXTENSION = ".log";
  
  private static final String FILE_EXTENSION_SEPERATOR = ".";
  
  private static final Pattern FILE_SIZE_PATTERN =
      Pattern.compile(
          "\\s*(\\d+)\\s*(k|g|m|)b?\\s*",
          Pattern.CASE_INSENSITIVE);

  private static final int BUFFER_SIZE = 8 * 1024;
  
  private File currentFile;

  private String fileName;

  private boolean isRotation = false;

  private String directory;

  private int maxFileSize;

  private int maxFileCount;

  private ArrayList<String> rotationFileNames;
  
  private PrintWriter writer = null;
  
  private boolean flushAfterWrite;
  private final ResourceLock lock = new ResourceLock();
  public LogFileHandler(String filename, 
  											boolean flushAfterWrite, 
  											String maxLogFileSize,
  											String maxLogFileCount) throws Exception
  {
      int separator = filename.lastIndexOf(File.separator);
      directory = filename.substring(0, separator);
      fileName = filename.substring(separator + 1);
      
      this.flushAfterWrite = flushAfterWrite;
      
      if (-1 != separator)
      {
          createDirectory();
      }
      
      createWriter(maxLogFileSize, maxLogFileCount);
  }
  
  @Override
  public void write(String message) throws Exception
  {
	  try (ResourceLock ignore = lock.obtain()) {
      writer.println(message);
      if (flushAfterWrite)
        writer.flush();
      if (isRotation)
      {
        writer.flush();
        if ((0 != maxFileSize) && (currentFile.length() >= maxFileSize))
        {
            closeFile();
            rotateFiles();
            openFile();
        }
      }
	  } 
  }
  
  @Override
  public void close() throws Exception {
	try (ResourceLock ignore = lock.obtain()) {
  	if (writer != null) {
  		writer.close();
  	}
	}
  }

  @Override
  public void flush() {
	try (ResourceLock ignore = lock.obtain()) {
  	if (writer != null) {
  		writer.flush();
  	}
	}
  }
  
  private void createDirectory() throws RedshiftException
  {
      File directory = new File(this.directory);
      if (!directory.exists() && !directory.mkdir())
      {
        throw new RedshiftException(GT.tr("Couldn't create log directory {0}", directory),
            					RedshiftState.UNEXPECTED_ERROR);
      }
  }
  
  private void createWriter(String maxLogFileSize, String maxLogFileCount) throws Exception
  {
    String fullFilename = directory + File.separator + fileName;
    if ((null != fullFilename) && (0 != fullFilename.length()))
    {
      BufferedOutputStream outStream = new BufferedOutputStream(
      																			new FileOutputStream(fullFilename, true),
      																			BUFFER_SIZE);
      writer = new PrintWriter(outStream);
      updateLoggingFileSettings(maxLogFileSize, maxLogFileCount);
      
      return;
    }
    
    throw new Exception("Failed to create log writer");
  }
  
  private void updateLoggingFileSettings(String maxLogFileSize, String maxLogFileCount)
  {
      currentFile = new File(directory + File.separator + fileName);

      // Read "maxLogFileCount"
      maxFileCount =  getMaxFileCount(maxLogFileCount);

      // Read "maxLogFileSize"
      maxFileSize = getMaxFileSize(maxLogFileSize);

      if (maxFileCount > 1)
      {
          isRotation = true;
          String fileExt = FILE_EXTENSION;
          
          rotationFileNames = new ArrayList<String>();
          if (fileName.contains(FILE_EXTENSION_SEPERATOR))
          {
              fileExt = fileName
                  .substring(fileName.lastIndexOf(FILE_EXTENSION_SEPERATOR), fileName.length());
              fileName = fileName.substring(0, fileName.lastIndexOf(FILE_EXTENSION_SEPERATOR));
          }
          
          rotationFileNames.add(directory + File.separator + fileName + fileExt);
          for (int i = 1; i < maxFileCount; i++)
          {
              // Name format: {basename}.{i}{.ext} : e.g. "redshift.1.log"
              rotationFileNames
                  .add(directory + File.separator + fileName + "." + i + fileExt);
          }
      }
  }
  
  private int getMaxFileSize(String maxLogFileSize)
  {
      int maxBytes = FILE_SIZE;
      if ((null == maxLogFileSize) || (maxLogFileSize.isEmpty()))
          return maxBytes;
      
      Matcher fileSizematcher = FILE_SIZE_PATTERN.matcher(maxLogFileSize);
      if (fileSizematcher.find())
      {
          try
          {
              maxBytes = Integer.valueOf(fileSizematcher.group(1)) *
                  						toMultiplier(fileSizematcher.group(2).toLowerCase());
          }
          catch (NumberFormatException e)
          {
          	writer.println(e.getMessage());
          	writer.flush();
          }
      }
      
      return maxBytes;
  }
  
  private int getMaxFileCount(String maxLogFileCount)
  {
  	int maxfileCount = FILE_COUNT;
  	
    if ((null != maxLogFileCount) 
    			&& (!maxLogFileCount.isEmpty()))
    {
        int fileCount = Integer.valueOf(maxLogFileCount);
        
        if (fileCount > 0)
        	maxfileCount = fileCount;
    }
      
    return maxfileCount;
  }
  
  private Integer toMultiplier(String sizeChar)
  {
      if (sizeChar.equals("g"))
      {
          return 1024 * 1024 * 1024;
      }
      else if (sizeChar.equals("m"))
      {
          return 1024 * 1024;
      }
      else if (sizeChar.equals("k"))
      {
          return 1024;
      }
      throw new NumberFormatException("Invalid file size unit.");
  }
  
  
  private boolean isOpen() {
  	return (writer != null);
  }
  
  private void openFile() throws FileNotFoundException
  {
      currentFile = new File(rotationFileNames.get(0));
      BufferedOutputStream outStream = new BufferedOutputStream(
										new FileOutputStream(rotationFileNames.get(0), true),
										BUFFER_SIZE);
      
      writer = new PrintWriter(outStream);
  }
  
  private void closeFile()
  {
      if (isOpen())
      {
          currentFile = null;
          if (null != writer)
          {
              writer.close();
          }
      }
  }
  
  private void rotateFiles() throws Exception
  {
      if(!(rotationFileNames.isEmpty()))
      	deleteOldestFile();
  }
  
  private void deleteOldestFile() throws IOException
  {
    String last = rotationFileNames.get(rotationFileNames.size() - 1);
    File file = new File(last);
    
    if (file.exists())
    {
        file.delete();
    }
    
    for (int i = rotationFileNames.size() - 2; i >= 0; i--)
    {
        File current;
        current = new File(rotationFileNames.get(i));
        if (current.exists())
        {
            File dest = new File(last);
            if (!current.renameTo(dest))
            {
                throw new IOException(
                    "can not rename file: " + current.getName() + " to: " + last);
            }
        }
        last = rotationFileNames.get(i);
    }
  }
}
