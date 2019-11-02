package com.ms.silverking.testing;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.EmbeddedSKConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.object.ObjectUtil;

public class ImmutableObjectTester {
    public ImmutableObjectTester() {
    }
    
    public void test(String className) {
        try {
            test(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void test(Class _class) {
        try {
            test(_class.newInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void test(Object instance) {
        Class   _class;
        
        _class = instance.getClass();
        System.out.printf("\n*** Test %s\n", _class.getName());
        try {
            Field[] fields;
            
            fields = getDeclaredFields(_class);
            //System.out.printf("fields.length %d\n", fields.length);
            for (Field field : fields) {
                testField(_class, instance, field);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private Object getMutationValue(Class fieldClass) {
        if (fieldClass == String.class) {
            return Long.toString(System.nanoTime());
        } else if (fieldClass == Long.class || fieldClass == long.class) {
            return new Long(System.nanoTime());
        } else if (fieldClass == Integer.class || fieldClass == int.class) {
            return new Integer((int)System.nanoTime());
        } else if (fieldClass == Short.class || fieldClass == short.class) {
            return new Short((short)System.nanoTime());
        } else if (fieldClass == Byte.class || fieldClass == byte.class) {
            return new Byte((byte)System.nanoTime());
        } else {
            //throw new RuntimeException("Unsupported fieldClass: "+ fieldClass.getName());
            System.out.printf("Can't test mutation: %s\n", fieldClass.getName());
            return null;
        }
    }
    
    private Class[] getAllFieldClasses(Class _class) {
        Field[] fields;
        Class[] fieldClasses;
        
        fields = getDeclaredFields(_class);
        fieldClasses = new Class[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldClasses[i] = fields[i].getType();
        }
        return fieldClasses;
    }
    
    private Method getMutator(Class _class, Field field) {
        try {
            Class[] args;
            
            args = new Class[1];
            args[0] = field.getType();
            return _class.getMethod(field.getName(), args);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private Method getAccessor(Class _class, Field field) {
        try {
            Class[] args;
            String  methodName;
            String  methodNameTemplate;
            
            args = new Class[0];
            methodNameTemplate = "get"+ field.getName();
            methodName = methodNameTemplate;
            for (Method method : getDeclaredMethods(_class)) {
                if (method.getName().equalsIgnoreCase(methodNameTemplate)) {
                    methodName = method.getName();
                    break;
                }
            }
            //System.out.println(methodName);
            return _class.getMethod(methodName, args);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    private Pair<Object,Boolean> mutate(Class _class, Object instance, Field field) {
        try {
            Method      mutator;
            Object[]    args;
            
            mutator = getMutator(_class, field);
            args = new Object[1];
            args[0] = getMutationValue(field.getType());
            return new Pair<>(mutator.invoke(instance, args), args[0] != null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private void testField(Class _class, Object instance, Field field) {
        Pair<Object,Boolean>    mutationResult;
        
        System.out.printf("testField %s %s\n", _class.getName(), field.getName());
        mutationResult = mutate(_class, instance, field);
        testAccess(_class, field, instance, mutationResult);
    }
    
    private void testAccess(Class _class, Field field, Object instance, Pair<Object,Boolean> mutationResult) {
        try {
            Method  accessor;
            Object  oldValue;
            Object  newValue;
            Object[]  args;
            
            accessor = getAccessor(_class, field);
            args = new Object[0];
            oldValue = accessor.invoke(instance, args);
            newValue = accessor.invoke(mutationResult.getV1(), args);
            System.out.printf("%s %s => %s\n", field.getName(), oldValue, newValue);
            if (mutationResult.getV2()) {
                if (ObjectUtil.equal(oldValue, newValue)) {
                    throw new RuntimeException("Mutation check failed");
                }
            } else {
                System.out.printf("No mutation check. Couldn't mutate.\n");
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Field[] getDeclaredFields(Class _class) {
        List<Field> declaredFields;
        
        //System.out.printf("getDeclaredFields %s\n", _class.getName());
        declaredFields = new ArrayList<>();
        for (Field field : _class.getDeclaredFields()) {
            int modifiers;
            
            modifiers = field.getModifiers();
            if (!Modifier.isStatic(modifiers)) {
                //System.out.printf("%30s\n", field.getName());
                declaredFields.add(field);
            }
        }
        return declaredFields.toArray(new Field[0]);
    }

    private Method[] getDeclaredMethods(Class _class) {
        List<Method> declaredMethods;
        
        declaredMethods = new ArrayList<>();
        for (Method method : _class.getDeclaredMethods()) {
            int modifiers;
            
            modifiers = method.getModifiers();
            if (!Modifier.isStatic(modifiers)) {
                declaredMethods.add(method);
            }
        }
        return declaredMethods.toArray(new Method[0]);
    }
    
    @Test
    public void test() {
        test(EmbeddedSKConfiguration.class);
        test(DHTConfiguration.emptyTemplate);
    }
    
    public static void main(String[] args) {
        try {
            ImmutableObjectTester   iot;
            
            iot = new ImmutableObjectTester();
            iot.test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
