package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SocketChannel;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.MessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.async.IncomingData;
import com.ms.silverking.net.async.ReadResult;
import com.ms.silverking.numeric.NumConversion;

// FUTURE - add header checksums, timeouts on reception

/**
 * Extension of IncomingData for MessageGroups.
 * 
 * This class is designed to receive data organized into ByteBuffers.
 * It first receives metadata regarding the buffers to be received,
 * after which it receives the buffers themselves.
 * 
 * Header format <fieldName> (<number of bytes>):
 * 	numberOfBuffers	(4)
 *  messageType (1)
 *  options		(3)
 *  uuid        (16)
 *  context     (8)
 * 
 * 	bufferLength[0]	(4)
 * 	...
 * 	bufferLength[numberOfBuffers - 1] (4)
 * 
 *  buffer[0]
 *  buffer[1]
 *  ...
 *
 */
public final class IncomingMessageGroup implements IncomingData {
    private final ByteBuffer    leadingBuffer;
	private ByteBuffer			bufferLengthsBuffer;
	private IntBuffer			bufferLengthsBufferInt;
	private int					curBufferIndex;
	private ByteBuffer[]		buffers;
	private int					lastNumRead;
	private ReadState			readState;
	private MessageType         messageType;
	private int					options;
	private UUIDBase            uuid;
	private long                context;
	private long                version; // FUTURE - unused
	private byte[]              originator;
	private int                 deadlineRelativeMillis;
	private ForwardingMode      forward;
		
	private enum ReadState{INIT_PREAMBLE_SEARCH, PREAMBLE_SEARCH, HEADER_LENGTH, BUFFER_LENGTHS, BUFFERS, DONE, CHANNEL_CLOSED};
	
    private static final int    maxBufferSize = Integer.MAX_VALUE;
    private static final int    errorTolerance = 4;
    
    // FUTURE - think about limiting number of buffers and message group size
    // see     public MessageGroup(MessageType messageType, long context, List<ByteBuffer> buffers)
    private static final int   maxNumBuffers = 65536; 
    private static final int   minNumBuffers = 1;
    
	private static final boolean	debug = false;
	
	public static void setClient() {
		//isClient = true;
	}

	public IncomingMessageGroup(boolean debug) {
	    // FUTURE - think about allocate direct here
        //leadingBuffer = ByteBuffer.allocateDirect(MessageFormat.leadingBufferSize);
	    leadingBuffer = ByteBuffer.allocate(MessageFormat.leadingBufferSize);
		readState = ReadState.INIT_PREAMBLE_SEARCH;
        //this.debug = debug;
	}
	
	public MessageType getMessageType() {
	    return messageType;
	}
	
	public UUIDBase getUUID() {
	    return uuid;
	}
	
	public long getContext() {
	    return context;
	}
	
	public byte[] getOriginator() {
	    return originator;
	}
	
	public int getDeadlineRelativeMillis() {
	    return deadlineRelativeMillis;
	}
	
	public long getVersion() {
	    if (version == 0) {
	        throw new RuntimeException("version not set");
	    }
	    return version;
	}
	
	public ByteBuffer[] getBuffers() {
		return buffers;
	}
	
	public MessageGroup getMessageGroup() {
	    MessageGroup   mg;
	    
	    for (ByteBuffer buffer : buffers) {
	        buffer.flip();
	    }
	    return new MessageGroup(messageType, options, uuid, context, buffers, originator, deadlineRelativeMillis, forward);
	}
	
	public int getLastNumRead() {
		return lastNumRead;
	}
	
    private boolean matchesStart(byte[] pattern, ByteBuffer buf) {
        if (debug) {
            for (int i = 0; i < 2; i++) {
                System.out.printf("\t%x\t%x", pattern[i], buf.get(i));
            }
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != buf.get(i)) {
                return false;
            }
        }
        return true;
    }
	
	public ReadResult readFromChannel(SocketChannel channel) throws IOException {
		int   numRead;
		int   readErrors;
		
		readErrors = 0;
		lastNumRead = 0;
		do {
			if (debug) {
				Log.fine(readState);
			}
			try {
    			switch (readState) {
                case INIT_PREAMBLE_SEARCH:
                    if (debug) {
                        Log.fine("leadingBuffer.clear()");
                    }
                    leadingBuffer.clear();
                    readState = ReadState.PREAMBLE_SEARCH; 
                    //break; fall through to PREAMBLE_SEARCH 
                case PREAMBLE_SEARCH:
                    numRead = channel.read(leadingBuffer);
                    if (debug) {
                        /*
                        byte[]  p;
                        
                        p = new byte[preambleLengthTypeContextBuffer.capacity()];
                        preambleLengthTypeContextBuffer.get(p);
                        */
                        Log.fine(numRead +"\t"+ leadingBuffer);
                                //+"\t"+ StringUtil.byteArrayToHexString(p));
                    }
                    if (numRead < 0) {
                        return ReadResult.CHANNEL_CLOSED;
                    }  
                    lastNumRead += numRead;
                    if (leadingBuffer.hasRemaining()) {
                        if (debug) {
                            Log.fine("Incomplete read result");
                        }
                        return ReadResult.INCOMPLETE;
                    } else {
                        byte[]  candidatePreamble;
                        
                        if (debug) {
                            Log.fine("Checking preamble match");
                        }
                        // we have a full preamble buffer, see if we match
                        //candidatePreamble = preambleAndHeaderLengthBuffer.array();
                        //if (Arrays.matchesStart(preamble, candidatePreamble)) {
                        if (matchesStart(MessageGroupGlobals.preamble, leadingBuffer)) {
                            long    uuidMSL;
                            long    uuidLSL;
                            
                            if (debug) {
                                Log.fine("preamble match found");
                            }
                            readState = ReadState.HEADER_LENGTH;
                            if (leadingBuffer.get(MessageFormat.protocolVersionOffset) != MessageGroupGlobals.protocolVersion) {
                                throw new RuntimeException("Unexpected protocolVersion: "+ MessageGroupGlobals.protocolVersion);
                            }
                            allocateBufferLengthsBuffer(leadingBuffer.getInt(MessageFormat.lengthOffset));
                            messageType = EnumValues.messageType[leadingBuffer.get(MessageFormat.typeOffset)];
                            options = leadingBuffer.get(MessageFormat.optionsOffset); // only support one byte of options presently; ignore the other 2
                            uuidMSL = leadingBuffer.getLong(MessageFormat.uuidMSLOffset);
                            uuidLSL = leadingBuffer.getLong(MessageFormat.uuidLSLOffset);
                            uuid = new UUIDBase(uuidMSL, uuidLSL);
                            if (debug) {
                                Log.fine("messageType: "+ messageType);
                            }
                            context = leadingBuffer.getLong(MessageFormat.contextOffset);
                            if (debug) {
                                System.out.printf("context %x\n", context);
                                Log.fine("context: "+ context);
                            }
                            originator = new byte[ValueCreator.BYTES]; 
                            leadingBuffer.position(MessageFormat.originatorOffset);
                            leadingBuffer.get(originator);
                            deadlineRelativeMillis = leadingBuffer.getInt(MessageFormat.deadlineRelativeMillisOffset);
                            // For debugging
                            //System.out.printf("%x:%x %s deadlineRelativeMillis %d\n",
                            //        uuidMSL, uuidLSL,
                            //        IPAddrUtil.addrAndPortToString(originator), deadlineRelativeMillis);
                            forward = EnumValues.forwardingMode[leadingBuffer.get(MessageFormat.forwardOffset)];
                            readState = ReadState.BUFFER_LENGTHS;
                            // FIXME - ADD FALLTHROUGH FOR THIS CASE
                        } else {
                            //if (debug) {
                                Log.warningAsync("*** No preamble match ***");
                            //}
                            /*
                            // mismatch - search for real preamble
                            leadingBuffer.clear();
                            //if (candidatePreamble[1] == preamble[0]) {
                            if (leadingBuffer.get(1) == MessageGroupGlobals.preamble[0]) {
                                leadingBuffer.put(MessageGroupGlobals.preamble[0]);
                            }
                            */
                            return ReadResult.ERROR;
                        }
                    }
                    break;
    			case BUFFER_LENGTHS:
    				if (bufferLengthsBuffer.remaining() <= 0) {
    					throw new IOException("bufferLengthsBuffer.remaining() <= 0");
    				}
    				numRead = channel.read(bufferLengthsBuffer);
    				if (debug) {
    					Log.fine("numRead ", numRead);
    				}
    				if (numRead < 0) {
    					return ReadResult.CHANNEL_CLOSED;
    				} else if (numRead == 0) {
    					return ReadResult.INCOMPLETE;
    				} else {
    					lastNumRead += numRead;
    					if (bufferLengthsBuffer.remaining() == 0) {
    						allocateBuffers();
    						readState = ReadState.BUFFERS;
    					}
    					break;
    				}
    			case BUFFERS:
    				ByteBuffer	curBuffer;
    				
    				// FIXME - MERGE THESE READS EVENTUALLY
    				curBuffer = buffers[curBufferIndex];
    				//if (curBuffer.remaining() <= 0) {
    				//	throw new IOException("curBuffer.remaining() <= 0");
    				//}
    				if (curBuffer.remaining() > 0) {
    					numRead = channel.read(curBuffer);
    				} else {
    					numRead = 0;
    				}
    				if (debug) {
    					Log.fine("numRead ", numRead);
    				}
    				if (numRead < 0) {
    					return ReadResult.CHANNEL_CLOSED;
    				} else if (numRead == 0) {
    					if (curBuffer.remaining() > 0) {
    						return ReadResult.INCOMPLETE;
    					} else {
    						curBufferIndex++;
    						assert curBufferIndex <= buffers.length;
    						if (curBufferIndex == buffers.length) {
    							readState = ReadState.DONE;
    							return ReadResult.COMPLETE;
    						} else {
    							break;
    						}
    					}
    				} else {
    					lastNumRead += numRead;
    					if (curBuffer.remaining() == 0) {
    						curBufferIndex++;
    						assert curBufferIndex <= buffers.length;
    						if (curBufferIndex == buffers.length) {
    							readState = ReadState.DONE;
    							return ReadResult.COMPLETE;
    						} else {
    							break;
    						}
    					} else {
    						break;
    					}
    				}
    			case DONE:
    			    if (debug) {
    			        Log.info("IncomingBufferedData.DONE");
    			    }
    				return ReadResult.COMPLETE;
                case CHANNEL_CLOSED:
                    throw new IOException("Channel closed");
    			default: throw new RuntimeException("panic");
    			}
			} catch (IOException ioe) {
	            if (debug) {
                    Log.logErrorWarning(ioe);
	            }
                if (ioe.getMessage().startsWith("Connection reset")) {
                    readState = ReadState.CHANNEL_CLOSED;
                    return ReadResult.CHANNEL_CLOSED;
                } else {
                    readErrors++;
    			    if (readErrors <= errorTolerance) {
    			        Log.logErrorWarning(ioe, "Ignoring read error "+ readErrors);
    			        leadingBuffer.clear();
    			        readState = ReadState.INIT_PREAMBLE_SEARCH;
    			        return ReadResult.INCOMPLETE;
    			    } else {
    			        throw ioe;
    			    }
                }
			}
		} while(true);
	}
	
	private void allocateBufferLengthsBuffer(int numBuffers) throws IOException {
		if (debug) {
			Log.fine("allocateBufferLengthsBuffer ", numBuffers);
		}
		if (numBuffers < minNumBuffers) {
			throw new IOException("numBuffers < "+ minNumBuffers);
		}
        if (numBuffers > maxNumBuffers) {
            throw new IOException("numBuffers > maxNumBuffers\t"
                                + numBuffers +" > "+ maxNumBuffers);
        }
		try {
		    bufferLengthsBuffer = ByteBuffer.allocate(numBuffers * NumConversion.BYTES_PER_INT);
        } catch (OutOfMemoryError oome) {
            Log.warning("OutOfMemoryError caught in buffer allocation");
            throw new IOException("OutOfMemoryError caught in buffer allocation");
        }
		bufferLengthsBufferInt = bufferLengthsBuffer.asIntBuffer();
		buffers = new ByteBuffer[numBuffers];
	}
	
	private void allocateBuffers() throws IOException {
		if (debug) {
			Log.fine("allocateBuffers ", buffers.length);
		}
		for (int i = 0; i < buffers.length; i++) {
			int	size;
			
			size = bufferLengthsBufferInt.get(i);
			if (size > maxBufferSize || size < 0) {
				throw new IOException("bad buffer size: "+ size);
			}
			if (debug) {
				Log.fine("allocating buffer: ", size);
			}
			try {
				buffers[i] = ByteBuffer.allocate(size);
                //buffers[i] = ByteBuffer.allocateDirect(size);
			} catch (OutOfMemoryError oome) {
				Log.warning("OutOfMemoryError caught in buffer allocation");
				throw new IOException("OutOfMemoryError caught in buffer allocation");
			}
		}
	}
	
	public String toString() {
	    StringBuilder  sb;
	    
	    sb = new StringBuilder();
	    sb.append("*********************************\n");
        sb.append(bufferToString(bufferLengthsBuffer));
        sb.append("buffers.length\t"+ buffers.length);
        for (ByteBuffer buffer : buffers) {
            sb.append(bufferToString(buffer));
        }
        sb.append(lastNumRead +"\t");
        sb.append(lastNumRead);
        sb.append(readState);
        sb.append("\n*********************************\n");
        return sb.toString();
	}
	
	public String bufferToString(ByteBuffer buf) {
	    if (buf == null) {
	        return "[null]";
	    } else {
    	    StringBuilder  sb;
    	    ByteBuffer     dup;
    	    
    	    dup = buf.duplicate();
    	    dup.rewind();
            sb = new StringBuilder();
            sb.append('[');
            sb.append(dup.limit());
            sb.append('\t');
    	    while (dup.remaining() > 0) {
    	        byte   b;
    	        
    	        b = dup.get();
    	        sb.append(Integer.toHexString(b) +":");
    	    }
            sb.append(']');
    	    return sb.toString();
	    }
	}
}
