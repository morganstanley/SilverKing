
package com.ms.silverking.text;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ms.silverking.cloud.dht.crypto.MD5Digest;
import com.ms.silverking.numeric.MutableInteger;


public class StringUtil {
    private static final int    defaultHexMinorGroupSize = 4;
    private static final int    defaultHexMajorGroupSize = 16;
    
    private static final char[] quoteChars = {'"', '\''};
    
    private static final String digits[] = {"0", "1", "2",
            "3", "4", "5", "6", "7", "8",
            "9", "a", "b", "c", "d", "e",
            "f"};
    
    private static final char   hexMajorDelim = ' ';
    private static final char   hexMinorDelim = ':';
    private static final char   delim = ' ';
    
    private static final String	defaultNullString = "<null>";
    
    public static boolean isNullOrEmpty(String s) {
    	return s == null || s.length() == 0;
    }
    
    public static boolean isNullOrEmptyTrimmed(String s) {
    	return s == null || s.trim().length() == 0;
    }
    
	public static String[] splitAndTrim(String source, String regex) {
		String[]	splitSource;
		
		//System.out.println("#"+ source +"#");
		splitSource = source.trim().split(regex);
		for (int i = 0; i < splitSource.length; i++) {
			splitSource[i] = splitSource[i].trim();
			//System.err.println("<"+ splitSource[i] +">");
		}
		return splitSource;
	}	
		
	public static String[] splitAndTrim(String source) {
		return splitAndTrim(source, "\\s+");
	}
	
	public static String toHexString(BigInteger x) {
	    return byteArrayToHexString(x.toByteArray());
	}
	
    public static String byteArrayToHexString(byte[] inBytes) {
        return byteArrayToHexString(inBytes, 0, inBytes.length);
    }
    
    public static String byteArrayToHexString(byte[] inBytes, int offset, int length) {
        return byteArrayToHexString(inBytes, offset, length, defaultHexMinorGroupSize, defaultHexMajorGroupSize);        
    }
    
	public static String byteArrayToHexString(byte[] inBytes, int offset, int length, int minorGroupSize, int majorGroupSize) {
//		if (inBytes == null) {
//			return byteBufferToHexString(null, minorGroupSize, majorGroupSize);
//		} else {
//			return byteBufferToHexString(ByteBuffer.wrap(inBytes, offset, length), minorGroupSize, majorGroupSize);
//		}

	  if (inBytes == null) {
            return "<null>";
        } else {
    	    StringBuilder out;
    						
    		out = new StringBuilder(inBytes.length * 2);
    		for (int i = 0; i < length; i++) {
    		    byte  curByte;
    		    
    		    curByte = inBytes[offset + i];
    			out.append(digits[(curByte & 0xF0) >>> 4]);
    			out.append(digits[curByte & 0x0f]);
                if ((i + 1) % minorGroupSize == 0) {
                    if (i % majorGroupSize == 0) {
                        out.append(hexMajorDelim);
                    } else {
                        out.append(hexMinorDelim);
                    }
                }
    		}

    		return out.toString();
        }
	}  	
	
    public static String byteBufferToHexString(ByteBuffer buf) {
        return byteBufferToHexString(buf, defaultHexMinorGroupSize, defaultHexMajorGroupSize);
    }
    
    public static String byteBufferToHexString(ByteBuffer buf, int minorGroupSize, int majorGroupSize) {
        if (buf == null) {
            return "<null>";
        } else {
            StringBuilder out;
                            
            out = new StringBuilder(buf.limit() * 2);
            for (int i = buf.position(); i < buf.limit(); i++) {
                byte curByte;
                
                curByte = buf.get(i);
                out.append(digits[(curByte & 0xF0) >>> 4]);
                out.append(digits[curByte & 0x0f]);
                if ((i + 1) % minorGroupSize == 0) {
                    if (i % majorGroupSize == 0) {
                        out.append(hexMajorDelim);
                    } else {
                        out.append(hexMinorDelim);
                    }
                }
            }
            return out.toString();
        }
    }   
    
    public static String byteBufferToString(ByteBuffer buf) {
        if (buf != null) {
            byte[]  b;
            
            b = new byte[buf.limit()];
            buf.get(b);
            String s = new String(b);
//            System.out.println("ts: '" + s + "'");
            return new String(b);
        } else {
            return "";
        }
    }
    
    public static ByteBuffer hexStringToByteBuffer(String s) {
//    	return ByteBuffer.wrap(new BigInteger(s).toByteArray());
        MutableInteger  sIndex;
        int     bIndex;
        byte[]  b;
        byte[]  b2;
        
        sIndex = new MutableInteger();
        bIndex = 0;
        b = new byte[s.length() * 2];
        while (sIndex.getValue() < s.length()) {
            byte    _b;
            String  _s;
            
            _s = nextByteChars(s, sIndex);
            if (_s != null) {
                _b = parseByte(_s);
                b[bIndex] = _b;
                bIndex++;
            }
        }
        b2 = new byte[bIndex];
        System.arraycopy(b, 0, b2, 0, bIndex);
        return ByteBuffer.wrap(b2);
    }
    
    public static boolean parseBoolean(String s) throws ParseException {
        if (s.equalsIgnoreCase("true")) {
            return true;
        } else if (s.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new ParseException("Not a valid boolean: "+ s, 0);
        }
    }
    
    public static byte parseByte(String s) {
//    	return (byte)Integer.parseInt(s, 16);
        if (s.length() != 2) {
            throw new RuntimeException("Must be two hex chars, not "+ s);
        } else {
            char    c0;
            char    c1;
            
            c0 = s.charAt(0);
            c1 = s.charAt(1);
            return (byte)((Character.digit(c0, 16) << 4) | (Character.digit(c1, 16)));
        }
    }
    
    static final String nextByteChars(String s, MutableInteger index) {
        StringBuilder   sb;
        int             startIndex;
        
        startIndex = index.getValue();
        sb = new StringBuilder();
        while (sb.length() < 2) {
            char    c;

            if (index.getValue() >= s.length()) {
                if (sb.length() > 0) {
                    throw new RuntimeException("Couldn't find two-char byte def starting at "+ startIndex);
                } else {
                    return null;
                }
            }
            c = s.charAt(index.getValue());
            if (isHexDigit(c)) {
                sb.append(c);
            } else if (c == hexMajorDelim || c == hexMinorDelim) {
                // skip this character
            } else {
                throw new RuntimeException("Bad char: "+ c);
            }
            index.increment();
        }
        return sb.toString();
    }
    
    static final boolean isHexDigit(char c) {
//    	return Character.digit(c, 16) >= 0;
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    }
    
    ///////////////////////////////////
    
	public static void display(String[][] sArray) {
		for (String[] member : sArray) {
			display(member);
		}
	}
	
    public static void display(List<String> list) {
        System.out.println( listToString(list) );
    }
	
    public static void display(List<String> list, char delim) {
        System.out.println( listToString(list, delim) );
    }
    
	public static void display(String[] sArray) {
		System.out.println( arrayToString(sArray) );
	}

	public static String arrayToString(String[][] sArray) {
	    StringBuilder	sBuf;
		
		sBuf = new StringBuilder();
		for (String[] subArray : sArray) {
			sBuf.append( arrayToString(subArray) );
		}
		return sBuf.toString();
	}
	
	public static String arrayToString(Object[] sArray) {
    	return getToString(Arrays.asList(sArray), delim);
//	    StringBuilder	sBuf;
//		
//		sBuf = new StringBuilder();
//		for (Object member : sArray) {
//			sBuf.append(member);
//			sBuf.append(delim);
//		}
//		return sBuf.toString();
	}

	public static String arrayToQuotedString(String[] sArray) {
    	return getToStringQuoted(Arrays.asList(sArray), delim);
//	    StringBuilder	sBuf;
//		
//		sBuf = new StringBuilder();
//		for (String member : sArray) {
//			sBuf.append('"');
//			sBuf.append(member);
//			sBuf.append("\" ");
//		}
//		return sBuf.toString();
	}

	public static String listToString(List<String> sArray) {
		return listToString(sArray, delim);
	}
	
	public static String listToString(List<? extends Object> sArray, char delim) {
    	return getToString(sArray, delim);
//	    StringBuilder	sBuf;
//		
//		sBuf = new StringBuilder();
//		for (Object member : sArray) {
//			sBuf.append(member);
//			sBuf.append(delim);
//		}
//		return sBuf.toString();
	}
	
    public static String toString(Collection<String> sArray) {
        return toString(sArray, delim);
    }
    
    public static String toString(Collection<? extends Object> sArray, char delim) {
    	return getToString(sArray, delim);
//        StringBuilder   sBuf;
//        
//        sBuf = new StringBuilder();
//        for (Object member : sArray) {
//            sBuf.append(member);
//            sBuf.append(delim);
//        }
//        return sBuf.toString();
    }

    private static <T> String getToString(Iterable<T> iter, char delim) {
    	return getToString(iter, "", "", delim);
    }
    
    private static <T> String getToStringQuoted(Iterable<T> iter, char delim) {
    	return getToString(iter, "\"", "\"", delim);	
    }
    
    private static <T> String getToString(Iterable<T> iter, String before, String after, char delim) {
    	StringBuilder   sBuf;
        
        sBuf = new StringBuilder();
        for (Object member : iter) {
        	sBuf.append(before);
            sBuf.append(member);
        	sBuf.append(after);
            sBuf.append(delim);
        }
        return sBuf.toString();
    }
    
	public static final boolean startsWithIgnoreCase(String target, String value) {
		if ( target.length() < value.length() ) {
			return false;
		} else {
			String	targetStart;
			targetStart = target.substring( 0, value.length() );
			return targetStart.equalsIgnoreCase(value);
		}
	}	
	
	public static String md5(String string) {
		MessageDigest	digest;
		
		digest = MD5Digest.getLocalMessageDigest();
		digest.update(string.getBytes(), 0, string.length());
		return StringUtil.byteArrayToHexString( digest.digest() );
	}	
	
	public static String trimLength(String s, int length) {
		return s.substring(0, Math.min(s.length(), length));
	}
	
	public static int countOccurrences(String string, char c) {
		return countOccurrences(string, c, Integer.MAX_VALUE);
	}
	
	public static int countOccurrences(String string, char c, int limit) {
		int	occurrences;
		
		occurrences = 0;
		limit = Math.min(limit, string.length());
		for (int i = 0; i < limit; i++) {
			if (string.charAt(i) == c) {
				occurrences++;
			}
		}
		return occurrences;
	}
	
	public static String replicate(char c, int n) {
		return replicate(c+"", n);
//		char[]	chars;
//		
//		chars = new char[n];
//		Arrays.fill(chars, c);
//		return new String(chars);
	}
	
	public static String replicate(String string, int n) {
	    StringBuilder	sBuf;
		
		sBuf = new StringBuilder();
		for (int i = 0; i < n; i++) {
			sBuf.append(string);
		}
		return sBuf.toString();
	}
	
	public static String[] splitAndTrimQuoted(String source) {
		ArrayList<String>	tokens;
		Pattern				pattern;
		Matcher				matcher;
		int					prevStart;
		int					prevEnd;
		
		prevEnd = 0;
		tokens = new ArrayList<String>();
		pattern = Pattern.compile("'((\\S+)(\\s*))*?\\S+'");
		matcher = pattern.matcher(source);
		while ( matcher.find() ) {
			String	token;
			
			if (matcher.start() > prevEnd) {
				String[]	unquotedTokens;
				
				unquotedTokens = splitAndTrim( source.substring(prevEnd, matcher.start()) );
				for (String ut : unquotedTokens) {
					if (ut.trim().length() > 0) {
						tokens.add(ut);
					}
				}
			}
			token = source.substring(matcher.start(), matcher.end());
			prevEnd = matcher.end();
			//System.out.println("# "+ token);
			if (token.trim().length() > 0) {
				tokens.add(token);
			}
		}
		if (prevEnd < source.length()) {
			String[]	unquotedTokens;
			
			unquotedTokens = splitAndTrim( source.substring(prevEnd) );
			for (String ut : unquotedTokens) {
				tokens.add(ut);
			}
		}
		return tokens.toArray(new String[0]);
	}
	
	public static List<String> projectColumn(List<String> src, int column, String regex, boolean filterNonexistent) {
		List<String>	projection;
		
		projection = new ArrayList<String>();
		for (String line : src) {
			String[]	columns;
			
			columns = line.split(regex);
			if (column < columns.length) {
				projection.add(columns[column]);
			} else {
				if (!filterNonexistent) {
					projection.add(null);
				}
			}
		}
		return projection;
	}
	
	public static String stripQuotes(String s) {
	    for (char c : quoteChars) {
	        if (s.charAt(0) == c) {
	            if (s.charAt(s.length() - 1) == c) {
	                s = s.substring(1, s.length() - 1);
	            } else {
	                throw new RuntimeException("Unmatched quote in "+ s);
	            }
	        } else {
                if (s.charAt(s.length() - 1) == c) {
                    throw new RuntimeException("Unmatched quote in "+ s);
                }
	        }
	    }
	    return s;
	}
	
	public static String escapeForRegex(String s) {
		// FUTURE - make this more complete
		return s.replaceAll("\\.", "\\.");
	}
	
	public static String nullSafeToString(Object o) {
		return nullSafeToString(o, defaultNullString);
	}
	
	public static String nullSafeToString(Object o, String nullString) {
		if (o == null) {
			return nullString;
		} else {
			return o.toString();
		}
	}
	
	public static void main(String[] args) {
	    /*
		String[]	qArgs;
		
		qArgs = splitAndTrimQuoted(args[0]);
		System.out.println("::: "+ args[0]);
		for (String arg : qArgs) {
			System.out.println(arg);
		}
		*/
	    String def;
	    ByteBuffer b1;
	    
	    def = "a37209d6:b213f516:67410756:4cb51d26";
	    b1 = hexStringToByteBuffer(def);
	    System.out.println(def);
	    System.out.println(byteBufferToHexString(b1));
	    
	    System.out.println();
        def = "a37209d6:b213f516:67410756:4cb51d26:";
        b1 = hexStringToByteBuffer(def);
        System.out.println(def);
        System.out.println(byteBufferToHexString(b1));
	}
}
