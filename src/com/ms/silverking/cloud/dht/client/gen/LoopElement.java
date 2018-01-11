package com.ms.silverking.cloud.dht.client.gen;


public class LoopElement implements Statement {
	private final Target	target;
	private final Position	position;
	
	private static final String	loopString = "ForAll";
	
	public static enum Position {Start, End};
	public static enum Target {Packages, Classes, Methods, StaticMethods, Parameters, Constructors, NonEmptyConstructors, ReferencedClasses, StaticFields, Enums, EnumValues, Interfaces, InheritedClasses};
	
	public LoopElement(Target target, Position position) {
		this.target = target;
		this.position = position;
	}
	
	public static LoopElement parse(String s) {
		int	i;
		
		i = s.indexOf(loopString);
		if (i < 0) {
			return null;
		} else {
			Position	position;
			Target		target;
			
			if (i == 0) {
				position = Position.Start;
			} else {
				if (i == Position.End.toString().length() && s.substring(0, i).equals(Position.End.toString())) {
					position = Position.End;
				} else {
					return null;
				}
			}
			target = Target.valueOf(s.substring(i + loopString.length()));
			return new LoopElement(target, position);
		}
	}
	
	public Target getTarget() {
		return target;
	}

	public Position getPosition() {
		return position;
	}
	
	@Override
	public String toString() {
		return String.format("Loop %s %s", target, position);
	}
}
