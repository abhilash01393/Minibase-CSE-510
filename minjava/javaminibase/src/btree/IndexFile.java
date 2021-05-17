package btree;

import java.io.*;
import global.*;



/**
 * Contains the enumerated types of state of the scan
 */
class ScanState 
{
  public static final int NEWSCAN = 0; 
  public static final int SCANRUNNING = 1; 
  public static final int SCANCOMPLETE = 2; 
}

/**
 * Base class for a index file
 */
public abstract class IndexFile 
{
}
