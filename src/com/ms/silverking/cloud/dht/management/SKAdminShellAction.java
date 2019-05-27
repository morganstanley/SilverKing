package com.ms.silverking.cloud.dht.management;

import java.util.ArrayList;
import java.util.List;

enum SKAdminShellAction {
    Mode, SyncData("sd", false), Target, Display, Help(false), Quit, ToggleVerbose("v", false), WaitForConvergence("w", false), 
    TestTarget("tt", false), RecoverData("rd", false), RequestChecksumTree("rct", false);
    
    public final String shortFormUpperCase;
    public final String shortFormLowerCase;
    public final String[] shortForms;
    
    private final boolean   usesReps;
    
    private SKAdminShellAction(String extraShortForms, boolean usesReps, boolean autoGenerateShortForms) {
        List<String>    _shortForms;
        
        _shortForms = new ArrayList<>();
        shortFormUpperCase = toShortFormUpperCase();
        shortFormLowerCase = shortFormUpperCase.toLowerCase();
        if (autoGenerateShortForms) {
	        _shortForms.add(shortFormUpperCase);
	        _shortForms.add(shortFormLowerCase);
        }
        if (extraShortForms != null) {
            String[]    _extraShortForms;
            
            _extraShortForms = extraShortForms.split("\\s+");
            for (String s : _extraShortForms) {
                _shortForms.add(s.toUpperCase());
                _shortForms.add(s);
            }
        }
        shortForms = _shortForms.toArray(new String[0]);
        this.usesReps = usesReps;
    }
    
    private SKAdminShellAction(String extraShortForms, boolean usesReps) {
    	this(extraShortForms, usesReps, true);
    }
    
    private SKAdminShellAction(String extraShortForms) {
        this(extraShortForms, true);
    }
    
    private SKAdminShellAction(boolean usesReps) {
        this(null, usesReps);
    }
    
    private SKAdminShellAction() {
        this(null);
    }
    
    public boolean isShortForm(String s) {
        for (int i = 0; i < shortForms.length; i++) {
            if (s.equals(shortForms[i])) {
                return true;
            }
        }
        return false;
    }
    
    public boolean usesReps() {
        return usesReps;
    }
    
    private String toShortFormUpperCase() {
        StringBuilder   shortForm;
        String          as;
        
        as = toString();
        shortForm = new StringBuilder();
        for (int i = 0; i < as.length(); i++) {
            char    c;
            
            c = as.charAt(i);
            if (Character.isUpperCase(c)) {
                shortForm.append(c);
            }
        }
        return shortForm.toString();
    }
    
    public static SKAdminShellAction parse(String def) {
        for (SKAdminShellAction action : SKAdminShellAction.values()) {
            if (action.isShortForm(def)) {
                return action;
            }
        }
        return SKAdminShellAction.valueOf(def);
    }

    private static String shortFormStrings(SKAdminShellAction a) {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(String.format("%-26s", a.toString()));
        for (String sf : a.shortForms) { 
            sb.append(String.format("    %-4s", sf));
        }
        return sb.toString();
    }
    
    public static String helpMessage() {
        StringBuilder   helpMessage;
        
        helpMessage = new StringBuilder();
        helpMessage.append(String.format("%-26s    %-4s %-4s\n", "Action", "Short forms", ""));
        for (SKAdminShellAction a : SKAdminShellAction.values()) {
            helpMessage.append(shortFormStrings(a));
            helpMessage.append('\n');
        }
        return helpMessage.toString();
    }
}