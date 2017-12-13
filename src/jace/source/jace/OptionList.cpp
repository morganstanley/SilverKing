#include "jace/OptionList.h"
using jace::OptionList;
using jace::Option;
using jace::SystemProperty;
using jace::Verbose;
using jace::JavaAgent;
using jace::CustomOption;
using jace::Hook;
using jace::VfprintfHook;
using jace::AbortHook;
using jace::ExitHook;

#include "jace/Jace.h"

#include <string>
using std::string;

#include <cstring>

#include <vector>
using std::vector;

#include <algorithm>
using std::copy;

#include <iterator>
using std::back_inserter;

OptionList::OptionList() : options()
{
}

void OptionList::push_back(const Option& option)
{
  OptionPtr ptr(option.clone());
  options.push_back(ptr);
}

size_t OptionList::size() const
{
  return options.size();
}

std::vector<OptionList::OptionPtr>::const_iterator OptionList::begin() const
{
  return options.begin();
}
 
std::vector<OptionList::OptionPtr>::const_iterator OptionList::end() const
{
  return options.end();
}

namespace
{
  char* stringDup(const char* str)
	{
    size_t length = strlen(str);
    char* newStr = new char[length + 1];
    strcpy(newStr, str);
    return newStr;
  }
}

JavaVMOption* OptionList::createJniOptions() const
{
  JavaVMOption* jniOptions = new JavaVMOption[size()];
  
	std::vector<OptionPtr>::const_iterator it = begin();
  std::vector<OptionPtr>::const_iterator end_it = end();

  for (int i = 0; it != end_it; ++it, ++i)
	{
    jniOptions[i].optionString = stringDup((*it)->stringValue().c_str());
    jniOptions[i].extraInfo = (*it)->extraInfo();
  }

  return jniOptions;
}


void OptionList::destroyJniOptions(JavaVMOption* jniOptions) const
{
  for (unsigned int i = 0; i < size(); ++i)
    delete[] (jniOptions[i].optionString);
  delete[] jniOptions;
}

SystemProperty::SystemProperty(const string& _name, const string& _value): 
  mName(_name), mValue(_value)
{
}

SystemProperty::SystemProperty(const SystemProperty& other) :
	mName (other.mName), mValue (other.mValue)
{
}

const string SystemProperty::name()
{
  return mName;
}
  
const string SystemProperty::value()
{
  return mValue;
}

const string SystemProperty::stringValue() const
{
  return "-D" + mName + "=" + mValue;
}

void* SystemProperty::extraInfo()
{
  return 0;
}

Option* SystemProperty::clone() const
{ 
  return new SystemProperty(mName, mValue); 
}

string Verbose::toString(Verbose::ComponentType componentType) const
{
	switch (componentType)
	{
		case GC:
			return "gc";
		case JNI:
			return "jni";
		case CLASS:
			return "class";
		default:
			throw JNIException("Unknown component: " + componentType);
	}
}

Verbose::Verbose(ComponentType _componentType) : componentType(_componentType)
{
}

Verbose::Verbose(const Verbose& other): 
	componentType(other.componentType)
{
}

const string Verbose::stringValue() const
{
  return string("-verbose:") + toString(componentType);
}

void* Verbose::extraInfo()
{
  return 0;
}

Option* Verbose::clone() const
{
  return new Verbose(componentType);
}

JavaAgent::JavaAgent(const string& _path):
	mPath(_path), mOptions("")
{
}

JavaAgent::JavaAgent(const string& _path, const string& _options) :
	mPath(_path), mOptions(trim(_options))
{
}

JavaAgent::JavaAgent(const JavaAgent& other):
	mPath(other.mPath), mOptions(other.mOptions)
{
}

string JavaAgent::trim(const string& text)
{
	// Trim Both leading and trailing spaces  
  size_t first = text.find_first_not_of(" \t"); // Find the first non-space character
  size_t last = text.find_last_not_of(" \t"); // Find the last non-space character
  
  // if all spaces or empty return an empty string
  if ((string::npos != first) && (string::npos != last))
		return text.substr(first, last - first + 1);
  return string();
}

const string JavaAgent::path()
{
	return mPath;
}

const string JavaAgent::options()
{
	return mOptions;
}

const string JavaAgent::stringValue() const
{
	string result = "-javaagent:" + mPath;
	if (mOptions != "")
		result += "=" + mOptions;
	return result;
}

void* JavaAgent::extraInfo()
{
  return 0;
}

Option* JavaAgent::clone() const
{
  return new JavaAgent(mPath, mOptions);
}

CustomOption::CustomOption(const string& _value) : value(_value)
{
}

CustomOption::CustomOption(const CustomOption& other):
	value(other.value)
{
}

const string CustomOption::stringValue() const
{
  return value;
}

void* CustomOption::extraInfo()
{
  return 0;
}

Option* CustomOption::clone() const
{
  return new CustomOption(value);
}


VfprintfHook::VfprintfHook(vfprintf_t _hook): hook(_hook)
{
}

const string VfprintfHook::stringValue() const
{
  return "vfprintf";
}

void* VfprintfHook::extraInfo()
{
  // Casting from a function pointer to an object pointer isn't valid C++
  // but JNI makes us do this.
  return (void*) hook;
}

Option* VfprintfHook::clone() const
{
  return new VfprintfHook(hook);
}

ExitHook::ExitHook(exit_t _hook): hook(_hook)
{
}

const string ExitHook::stringValue() const
{
  return "exit";
}

void* ExitHook::extraInfo()
{
  // Casting from a function pointer to an object pointer isn't valid C++
  // but JNI makes us do this.
  return (void*) hook;
}

Option* ExitHook::clone() const
{
  return new ExitHook(hook);
}

AbortHook::AbortHook(abort_t _hook): hook(_hook)
{
}

const string AbortHook::stringValue() const
{
  return "abort";
}

void* AbortHook::extraInfo()
{
  // Casting from a function pointer to an object pointer isn't valid C++
  // but JNI makes us do this.
  return (void*) hook;
}

Option* AbortHook::clone() const
{
  return new AbortHook(hook);
}
