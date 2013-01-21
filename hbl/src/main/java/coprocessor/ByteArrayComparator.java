package coprocessor;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Simple byte array comparator with asc/desc
 * 
 * @author michael
 *
 */
public class ByteArrayComparator implements Comparator<byte[]>, Serializable {
  private static final long serialVersionUID = 1L;
  private boolean asc = true;
  
  public ByteArrayComparator(boolean _asc) {
	  asc = _asc;
  }
  
  @Override
  public int compare(byte[] o1, byte[] o2) {
    
    int minLen = Math.min(o1.length, o2.length);
    
    for (int i = 0; i < minLen; i++) {
      int a = (o1[i] & 0xff);
      int b = (o2[i] & 0xff);
      
      if (a != b) {
    	  if(asc) {
    		  return (a - b);
    	  }
    	  return -(a - b);
      }
    }
    
    return o1.length - o2.length;
  }
}