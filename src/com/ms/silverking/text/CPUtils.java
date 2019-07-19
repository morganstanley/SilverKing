package com.ms.silverking.text;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Static methods used by ClassParser during parsing. Placed here to allow ClassParser to be leaner. 
 */
class CPUtils {
    private static final boolean    debug = false;
    
    static Field[] getDeclaredFields(Class _class) {
        return getDeclaredFieldsList(_class).toArray(new Field[0]);
    }
    
    static List<Field> getDeclaredFieldsList(Class _class) {
        List<Field> fields;
        Class   superClass;
        
        fields = new ArrayList<>();
        fields.addAll(Arrays.asList(_class.getDeclaredFields()));
        superClass = _class.getSuperclass();
        if (superClass != Object.class) {
            List<Field> superFields;

            superFields = getDeclaredFieldsList(superClass);
            superFields.addAll(fields);
            return superFields;
        } else {
            return fields;
        }
    }
    
    static <T> Constructor<T> getConstructor(Class _class, Class[] fields) throws NoSuchMethodException {
        Constructor<T>[]    constructors;
        
        if (ObjectDefParser2.debug) {
            Thread.dumpStack();
            System.out.printf("\n\n *** getConstructor(%s)\n", _class.getName());
        }
        constructors = _class.getConstructors();
        for (Constructor<T> constructor : constructors) {
            if (constructorMatches(constructor, fields)) {
                return constructor;
            }
        }
        throw new NoSuchMethodException();
    }
    
    static <T> boolean constructorMatches(Constructor<T> constructor, Class[] fields) {
        Class[] cFields;
     
        cFields = constructor.getParameterTypes();
        if (debug) {
            System.out.printf("\ncFields\n%s\nfields\n%s\n\n", StringUtil.arrayToString(cFields), StringUtil.arrayToString(fields));
        }
        if (fields.length != cFields.length) {
            if (debug) {
                System.out.printf("Returning false.....................\n");
            }
            return false;
        } else {
            for (int i = 0; i < fields.length; i++) {
                if (!cFields[i].isAssignableFrom(fields[i])) {
                    if (debug) {
                        System.out.printf("%s isInstance %s failed\n", cFields[i], fields[i]);
                    }
                    if (debug) {
                        System.out.printf("Returning false.....................\n");
                    }
                    return false;
                }
            }
            if (ObjectDefParser2.debug || debug) {
                System.out.printf("Returning true.....................\n");
            }
            return true;
        }
    }
    
    static Field[] filterStaticFields(Field[] fields) {
        List<Field> nonStatic;
        
        nonStatic = new ArrayList<>();
        for (Field field : fields) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                nonStatic.add(field);
            }
        }
        return nonStatic.toArray(new Field[0]);
    }
    
    static Field[] filterFields(Field[] fields, Set<String> filterSet) {
        List<Field> passed;
        
        passed = new ArrayList<>();
        for (Field field : fields) {
            if (!filterSet.contains(field.getName())) {
                passed.add(field);
            }
        }
        return passed.toArray(new Field[0]);
    }
    
    static Class<?>[] getFieldClasses(Field[] fields) {
        Class<?>[] classes;
        
        classes = new Class[fields.length];
        for (int i = 0; i < fields.length; i++) {
            classes[i] = fields[i].getType();
        }
        if (debug) {
            System.out.println(".....");
            for (Class c : classes) {
                System.out.println(c);
            }
        }
        return classes;
    }
    
    
}
