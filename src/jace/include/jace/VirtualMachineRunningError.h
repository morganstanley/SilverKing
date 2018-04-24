#ifndef JACE_VIRTUAL_MACHINE_RUNNING_ERROR_H
#define JACE_VIRTUAL_MACHINE_RUNNING_ERROR_H

#include "jace/JNIException.h"

BEGIN_NAMESPACE(jace)


/**
 * An operation has failed because the virtual machine is running.
 *
 * @author Gili Tzabari
 */
class VirtualMachineRunningError: public ::jace::JNIException
{
public:
	/**
	 * Creates a new VirtualMachineRunningError with the given message.
	 */
	VirtualMachineRunningError(const std::string& value) throw ():
			JNIException(value)
	{}
};

END_NAMESPACE(jace)

#endif // #ifndef JACE_VIRTUAL_MACHINE_RUNNING_ERROR_H

