#ifndef JACE_JARRAY_H
#define JACE_JARRAY_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClassImpl.h"
#include "jace/ElementProxy.h"
#include "jace/JArrayHelper.h"
#include "jace/JNIException.h"
#include "jace/proxy/types/JBoolean.h"
#include "jace/proxy/types/JByte.h"
#include "jace/proxy/types/JChar.h"
#include "jace/proxy/types/JDouble.h"
#include "jace/proxy/types/JFloat.h"
#include "jace/proxy/types/JInt.h"
#include "jace/proxy/types/JLong.h"
#include "jace/proxy/types/JShort.h"

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

#include <string>
#include <vector>

BEGIN_NAMESPACE(jace)


/**
 * Represents an array of JValues.
 *
 * A JArray may be constructed with any type that matches
 * the JValue interface. JArrays act like any other array.
 * For example:
 *
 *   // Creates an empty array of type String of size 10.
 *   JArray<String> myArray(10);
 *
 *   // Sets the 3rd element to the String, "Hello World".
 *   myArray[3] = String("Hello World");
 *
 *   // Retrieves the String, "Hello World" from the array.
 *   String hw = myArray[3];
 *
 * @author Toby Reyelts
 *
 */
template <class ElementType> class JArray: public ::jace::proxy::JObject
{
public:
	/**
	 * Constructs a new JArray from the given JNI array.
	 */
	JArray(jvalue array): JObject(0)
	{
		this->setJavaJniValue(array);
		this->_length = -1;
	}


	/**
	 * Constructs a new JArray from the given JNI array.
	 */
	JArray(jobject array): JObject(0)
	{
		this->setJavaJniObject(array);
		this->_length = -1;
	}


	/**
	 * Constructs a new JArray from the given JNI array.
	 */
	JArray(jarray array): JObject(0)
	{
		this->setJavaJniObject(array);
		this->_length = -1;
	}


	/**
	 * Constructs a new JArray of the given size.
	 */
	JArray(int size): JObject(0)
	{
		jobject localRef = ::jace::JArrayHelper::newArray(size, ElementType::staticGetJavaJniClass());
		this->setJavaJniObject(localRef);
		JNIEnv* env = attach();
		deleteLocalRef(env, localRef);
		_length = size;
	}


	/**
	 * Creates a new null reference.
	 *
	 * All subclasses of JArray should provide this constructor
	 * for their own subclasses.
	 */
	JACE_API explicit JArray();

	/**
	 * Creates a new JArray from a vector of a convertible type, T.
	 */
	template <class T> JArray(const std::vector<T>& values): JObject(0)
	{
		#ifdef NO_IMPLICIT_TYPENAME
			#define TYPENAME typename
		#else
			#define TYPENAME
		#endif

		jobjectArray localArray = ::jace::JArrayHelper::newArray(values.size(), ElementType::staticGetJavaJniClass());
		this->setJavaJniObject(localArray);

		int i = 0;
		JNIEnv* env = attach();

		for (TYPENAME std::vector<T>::const_iterator it = values.begin(); it != values.end(); ++it, ++i)
		{
			env->SetObjectArrayElement(localArray, i, ElementType(*it));
			catchAndThrow();
		}
		_length = values.size();
		deleteLocalRef(env, localArray);
	}

	JArray(const JArray& array): JObject(0)
	{
		this->setJavaJniObject(array);
		this->_length = array._length;
	}

	/**
	 * Destroys this JArray.
	 */
	~JArray() throw ()
	{}

	/**
	 * Retrieves the length of the array.
	 *
	 */
	::jace::proxy::types::JInt length() const
	{
		#ifdef JACE_CHECK_NULLS
			if (!static_cast<jobject>(*this))
				throw ::jace::JNIException("[JArray::length] Can not retrieve the length of a null array.");
		#endif

		if (_length == -1)
			_length = ::jace::JArrayHelper::getLength(static_cast<jobject>(*this));
		return _length;
	}


	/**
	 * Retrieves the element at the given index of the array.
	 *
	 * @throw ArrayIndexOutOfBoundsException if the index
	 * is outside of the range of the array.
	 *
	 * @internal This method needs to return a 'proxy ElementType' that, if assigned to,
	 * automatically pins and depins that single element in the array.
	 */
	ElementProxy<ElementType> operator[](const int& index)
	{
		#ifdef JACE_CHECK_NULLS
			if (!static_cast<jobject>(*this))
				throw ::jace::JNIException("[JArray::operator[]] Can not dereference a null array.");
		#endif

		#ifdef JACE_CHECK_ARRAYS
			if (index >= length())
				throw ::jace::JNIException("[JArray::operator[]] invalid array index.");
		#endif

		jvalue localElementRef = ::jace::JArrayHelper::getElement(static_cast<jobject>(*this), index);
		ElementProxy<ElementType> element(this->getJavaJniArray(), localElementRef, index);
		JNIEnv* env = attach();
		deleteLocalRef(env, localElementRef.l);
		return element;
	}

	/**
	 * An overloaded version of operator[] that works for const
	 * instances of JArray.
	 */
	const ElementProxy<ElementType> operator[](const int& index) const
	{
		#ifdef JACE_CHECK_NULLS
			if (!static_cast<jobject>(*this))
				throw ::jace::JNIException("[JArray::operator[]] Can not dereference a null array.");
		#endif

		#ifdef JACE_CHECK_ARRAYS
			if (index >= length())
				throw ::jace::JNIException("[JArray::operator[]] invalid array index.");
		#endif

		jvalue localElementRef = ::jace::JArrayHelper::getElement(static_cast<jobject>(*this), index);
		ElementProxy<ElementType> element(this->getJavaJniArray(), localElementRef, index);
		JNIEnv* env = attach();
		deleteLocalRef(env, localElementRef.l);
		return element;
	}


	/**
	 * Returns the JClass for this instance.
	 *
	 * @throw JNIException if an error occurs while trying to retrieve the class.
	 */
	virtual const ::jace::JClass& getJavaJniClass() const throw (::jace::JNIException)
	{
		return JArray<ElementType>::staticGetJavaJniClass();
	}


	/**
	 * Returns the JClass for this class.
	 *
	 * @throw JNIException if an error occurs while trying to retrieve the class.
	 */
	static const ::jace::JClass& staticGetJavaJniClass() throw (JNIException)
	{
		static boost::shared_ptr<JClassImpl> result;
		boost::mutex::scoped_lock lock(javaClassMutex);
		if (result == 0)
		{
			const std::string signature = "[" + ElementType::staticGetJavaJniClass().getSignature();

			// The internal name of an array is equal to its signature
			//
			// REFERENCE: http://download.oracle.com/javase/6/docs/technotes/guides/jni/spec/functions.html#wp16027
			const std::string internalName = signature;

			result = boost::shared_ptr<JClassImpl>(new JClassImpl(internalName, signature));
		}
		return *result;
	}


	/**
	 * Returns the JNI jarray handle for this array.
	 */
	jarray getJavaJniArray() const
	{
		return static_cast<jarray>(static_cast<jobject>(*this));
	}


	/**
	 * Returns the JNI jarray handle for this array as a non-const handle.
	 */
	jarray getJavaJniArray()
	{
		return static_cast<jarray>(static_cast<jobject>(*this));
	}

	/**
	 * An Iterator class for use with the standard C++ library. For
	 * example you can use stl::copy() to copy the contents of a JArray:
	 *
	 * <code>
	 * JArray<JBoolean> javaArray = ...;
	 * jboolean* nativeArray = new jboolean[javaArray.size()];
	 *
	 * std::copy(myArray.begin(), myArray.end(), nativeArray);
	 * </code>
	 *
	 * Iterator should be preferred to operator[] for non-random
	 * access of arrays, as it allows Jace to perform smart caching
	 * against the array accesses.
	 *
	 * Note that an Iterator is only good for as long as it's parent
	 * is alive. Accessing an Iterator after the destruction of the
	 * parent array causes undefined behavior.
	 */
	class Iterator: public std::iterator<std::random_access_iterator_tag, ElementType>
	{
	public:
		Iterator(JArray<ElementType>* parent_, int _begin, int _end) :
			parent(parent_),
			current(_begin),
			end(_end)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (_begin < 0 || _begin > parent->length())
					throw ::jace::JNIException("[JArray::Iterator::Iterator] begin is out of bounds.");
				if ((_end < _begin && _end != -1) || end > parent->length())
					throw ::jace::JNIException("[JArray::Iterator::Iterator] end is out of bounds.");
			#endif

			parent->cache(current, end);
		}

		Iterator(const Iterator& it):
			parent(it.parent),
			current(it.current),
			end(it.end)
		{}

		~Iterator()
		{
			parent->release(current, end);
		}

		Iterator operator=(const Iterator& it)
		{
			parent = it.parent;
			current = it.current;
			end = it.end;
			return *this;
		}

		bool operator==(const Iterator& it)
		{
			return (parent == it.parent && current == it.current);
		}

		bool operator!=(const Iterator& it)
		{
			return !(*this == it);
		}

		bool operator<(const Iterator& it)
		{
			return (parent == it.parent && current < it.current);
		}

		bool operator<=(const Iterator& it)
		{
			return (parent == it.parent && current <= it.current);
		}

		bool operator>(const Iterator& it)
		{
			return (parent == it.parent && current > it.current);
		}

		bool operator>=(const Iterator& it)
		{
			return (parent == it.parent && current >= it.current);
		}

		// pre
		Iterator operator++()
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current >= parent->length())
					throw ::jace::JNIException("[JArray::Iterator::operator++] can not advance iterator out of bounds.");
			#endif

			++current;
			return *this;
		}

		// post
		Iterator operator++(int dummy)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current >= parent->length())
					throw ::jace::JNIException("[JArray::Iterator::operator++] can not advance iterator out of bounds.");
			#endif

			Iterator it(*this);
			++current;

			return it;
		}

		Iterator operator+=(int i)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current + i > parent->length())
					throw ::jace::JNIException("[JArray::Iterator::operator+=] can not advance iterator out of bounds.");
			#endif

			current += i;
			return *this;
		}

		// pre
		Iterator operator--()
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current == 0)
					throw ::jace::JNIException("[JArray::Iterator::operator--] can not rewind iterator out of bounds.");
			#endif

			--current;
			return *this;
		}

		// post
		Iterator operator--(int dummy)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current == 0)
					throw ::jace::JNIException("[JArray::Iterator::operator--] can not rewind iterator out of bounds.");
			#endif

			Iterator it(*this);
			--current;

			return it;
		}

		Iterator operator-=(int i)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current - i < 0)
					throw ::jace::JNIException("[JArray::Iterator::operator-=] can not rewind iterator out of bounds.");
			#endif

			current -= i;
			return *this;
		}

		Iterator operator+(int i)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current + i > parent->length())
					throw ::jace::JNIException("[JArray::Iterator::operator+] can not advance iterator out of bounds.");
			#endif

			Iterator it(*this);
			it.current += i;
			return it;
		}

		Iterator operator-(int i)
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current - i < 0)
					throw ::jace::JNIException("[JArray::Iterator::operator-] can not rewind iterator out of bounds.");
			#endif

			Iterator it(*this);
			it.current -= i;
			return it;
		}

		operator int()
		{
			return current;
		}

		/**
		 * Returns the element to which the Iterator points.
		 */
		ElementProxy<ElementType> operator*()
		{
			#ifdef JACE_CHECK_ARRAYS
				if (current < 0 || current >= parent->length())
					throw ::jace::JNIException("[JArray::Iterator::operator*] can not dereference an out of bounds iterator.");
			#endif

			// Change to use caching in the future
			return parent->operator[](current);
		}
	private:
		JArray<ElementType>* parent;
		int current;
		int end;
	};

		/**
		 * Returns an Iterator to the array at position, <code>start</code>
		 *
		 * @param start The position at which to place the iterator.
		 * @param end The position to which you are likely to iterate to.
		 * This should be equal to or larger than start, or may be set to -1
		 * to indicate the end of the array.
		 */
		Iterator begin(int start = 0, int end = -1)
		{
			return Iterator(this, start, end);
		}

		/**
		 * Returns an Iterator at one past the end of the array.
		 * This Iterator should not be dereferenced.
		 */
		Iterator end()
		{
			return Iterator(this, length(), length());
		}
private:
	/**
	 * Disallow operator= for now.
	 */
	bool operator=(const JArray& array);

	/**
	 * Disallow operator== for now.
	 */
	bool operator==(const JArray& array);

	// Methods for future implementation of caching
	void cache(int begin, int end)
	{}

	void release(int begin, int end)
	{}

	friend class Iterator;


	// The cached length of the array.
	// Mutable, because it's calculation can be deferred.
	mutable int _length;
	static boost::mutex javaClassMutex;
};

template <class ElementType> boost::mutex JArray<ElementType>::javaClassMutex;

END_NAMESPACE(jace)

/**
 * For those (oddball) compilers that need the template specialization
 * definitions in the header.
 */
#ifdef PUT_TSDS_IN_HEADER
  #include "jace/JArray.tsd"
#else
  #include "jace/JArray.tsp"
#endif

#endif // #ifndef JACE_JARRAY_H

