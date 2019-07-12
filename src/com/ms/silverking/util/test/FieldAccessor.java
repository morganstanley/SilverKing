package com.ms.silverking.util.test;

import java.lang.reflect.Field;

public class FieldAccessor {
    ////////////
    // setters
    
    public static <T> void set(Object instance, String fieldName, T value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, byte value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, short value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, int value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, long value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, float value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, double value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, boolean value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void set(Object instance, String fieldName, char value) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                f.set(instance, value);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    ////////////
    // getters
    
    public static <T> T getObject(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return (T)f.get(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static byte getByte(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getByte(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static short getShort(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getShort(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static int getInt(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getInt(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static long getLong(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getLong(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static float getFloat(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getFloat(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static double getDouble(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getDouble(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static boolean getBoolean(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getBoolean(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
    
    public static char getChar(Object instance, String fieldName) {
        Testing.ensureTestingEnabled();
        try {
            Field    f;
            boolean    accessible;
            
            f = instance.getClass().getDeclaredField(fieldName);
            accessible = f.isAccessible();
            try {
                if (!accessible) {
                    f.setAccessible(true);
                } 
                return f.getChar(instance);
            } finally {
                if (!accessible) {
                    f.setAccessible(false);
                } 
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
}
