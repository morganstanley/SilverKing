#ifndef JACE_JFIELD_PROXY_H
#define JACE_JFIELD_PROXY_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "JFieldProxyHelper.h"
#include "jace/proxy/types/JBoolean.h"
#include "jace/proxy/types/JByte.h"
#include "jace/proxy/types/JChar.h"
#include "jace/proxy/types/JDouble.h"
#include "jace/proxy/types/JFloat.h"
#include "jace/proxy/types/JInt.h"
#include "jace/proxy/types/JLong.h"
#include "jace/proxy/types/JShort.h"

BEGIN_NAMESPACE(jace)

/**
 * A JFieldProxy is a wrapper around a JField.
 *
 * A JFieldProxy makes sure that assignments can happen to class
 * and instance fields. For example,
 *
 *
 * // Java class
 * public class Foo
 * {
 *   public String bar;
 * }
 *
 * // C++ proxy class
 * class Foo: public Object
 * {
 * public:
 *   JFieldProxy<String> bar();
 * }
 *
 * // C++ code.
 * Foo.bar() = String("Hello!");
 *
 * @author Toby Reyelts
 *
 */
template <class FieldType> class JFieldProxy: public FieldType
{
public:
	/**
	 * Creates a new JFieldProxy that belongs to the given object,
	 * and represents the given value.
	 *
	 * This constructor should always be specialized away by subclasses.
	 */
	JFieldProxy(jfieldID _fieldID, jvalue value, jobject _parent):
		FieldType(value), fieldID(_fieldID)
	{
		JNIEnv* env = attach();

		if (_parent)
			parent = newGlobalRef(env, _parent);
		else
			parent = _parent;

		parentClass = 0;
	}


	/**
	 * Creates a new JFieldProxy that belongs to the given class,
	 * and represents the given value. (The field is a static one).
	 *
	 * This constructor should always be specialized away by subclasses.
	 */
	JFieldProxy(jfieldID _fieldID, jvalue value, jclass _parentClass):
		FieldType(value), fieldID(_fieldID)
	{
		parent = 0;
		JNIEnv* env = attach();
		parentClass = newGlobalRef(env, _parentClass);
	}


	/**
	 * Creates a new JFieldProxy that belongs to the given object,
	 * and represents the given value.
	 *
	 * This copy constructor should always be specialized away by subclasses.
	 */
	JFieldProxy(const JFieldProxy& object):
		FieldType(object.getJavaJniValue())
	{
		JNIEnv* env = attach();
		if (object.parent)
			parent = newGlobalRef(env, object.parent);
		else
			parent = 0;

		if (object.parentClass)
			parentClass = static_cast<jclass>(newGlobalRef(env, object.parentClass));
		else
			parentClass = 0;
	}


	virtual ~JFieldProxy() throw()
	{
		if (parent)
		{
			try
			{
				JNIEnv* env = attach();
				deleteGlobalRef(env, parent);
			}
			catch (std::exception&)
			{
			}
		}

		if (parentClass)
		{
			try
			{
				JNIEnv* env = attach();
				deleteGlobalRef(env, parentClass);
			}
			catch (std::exception&)
			{
			}
		}
	}

	/**
	 * If someone assigns to this proxy, they're really assigning
	 * to the field.
	 */
	FieldType& operator=(const FieldType& field)
	{
		if (parent)
			setJavaJniObject(JFieldProxyHelper::assign(field, parent, fieldID));
		else
			setJavaJniObject(JFieldProxyHelper::assign(field, parentClass, fieldID));
		return *this;
	}

private:
	jobject parent;
	jclass parentClass;
	jfieldID fieldID;
};

END_NAMESPACE(jace)

/**
 * For those (oddball) compilers that need the template specialization
 * definitions in the header.
 */
#ifdef PUT_TSDS_IN_HEADER
  #include "jace/JFieldProxy.tsd"
#else
  #include "jace/JFieldProxy.tsp"
#endif

#endif // #ifndef JACE_JFIELD_PROXY_H
