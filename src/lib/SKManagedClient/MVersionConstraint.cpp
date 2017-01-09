#include "StdAfx.h"
#include "MVersionConstraint.h"

#include <stdlib.h>
#include <string>
using namespace std;
#include "SKVersionConstraint.h"


namespace SKManagedClient {

MVersionConstraint ^ MVersionConstraint::exactMatch(Int64 version)
{
	SKVersionConstraint * pvc = SKVersionConstraint::exactMatch(version);
	SKVersionConstraint_M ^ vc = gcnew SKVersionConstraint_M;
	vc->pVersionConstraint = pvc;
	return gcnew MVersionConstraint(vc);
}

MVersionConstraint ^ MVersionConstraint::maxAboveOrEqual(Int64 threshold)
{
	SKVersionConstraint * pvc = SKVersionConstraint::maxAboveOrEqual(threshold);
	SKVersionConstraint_M ^ vc = gcnew SKVersionConstraint_M;
	vc->pVersionConstraint = pvc;
	return gcnew MVersionConstraint(vc);
}

MVersionConstraint ^ MVersionConstraint::maxBelowOrEqual(Int64 threshold)
{
	SKVersionConstraint * pvc = SKVersionConstraint::maxBelowOrEqual(threshold);
	SKVersionConstraint_M ^ vc = gcnew SKVersionConstraint_M;
	vc->pVersionConstraint = pvc;
	return gcnew MVersionConstraint(vc);
}

MVersionConstraint ^ MVersionConstraint::minAboveOrEqual(Int64 threshold)
{
	SKVersionConstraint * pvc = SKVersionConstraint::minAboveOrEqual(threshold);
	SKVersionConstraint_M ^ vc = gcnew SKVersionConstraint_M;
	vc->pVersionConstraint = pvc;
	return gcnew MVersionConstraint(vc);
}

MVersionConstraint::MVersionConstraint(Int64 minVersion, Int64 maxVersion, SKVersionConstraintMode_M mode, Int64 maxStorageTime)
{
	SKVersionConstraint * pvc = new SKVersionConstraint( minVersion, maxVersion, (SKVersionConstraintMode) mode, maxStorageTime );
	pImpl = (void *) pvc;
}

MVersionConstraint::MVersionConstraint(Int64 minVersion, Int64 maxVersion, SKVersionConstraintMode_M mode)
{
	SKVersionConstraint * pvc = new SKVersionConstraint( minVersion, maxVersion, (SKVersionConstraintMode) mode );
	pImpl = (void *) pvc;
}

MVersionConstraint::~MVersionConstraint()
{
	this->!MVersionConstraint();
}

MVersionConstraint::!MVersionConstraint()
{
	if(pImpl)
	{
		delete (SKVersionConstraint*)pImpl ;
		pImpl = NULL;
	}
}

MVersionConstraint::MVersionConstraint(SKVersionConstraint_M ^ verConstraint)
{
	pImpl = verConstraint->pVersionConstraint;
}

SKVersionConstraint_M ^ MVersionConstraint::getPImpl()
{
	SKVersionConstraint_M ^ verConstr = gcnew SKVersionConstraint_M;
	verConstr->pVersionConstraint = pImpl;
	return verConstr;
}

Int64 MVersionConstraint::getMax()
{
	Int64 mx = ((SKVersionConstraint*)pImpl)->getMax();
	return mx;
}

Int64 MVersionConstraint::getMaxCreationTime()
{
	Int64 mx = ((SKVersionConstraint*)pImpl)->getMaxCreationTime();
	return mx;
}

Int64 MVersionConstraint::getMin()
{
	Int64 mn = ((SKVersionConstraint*)pImpl)->getMin();
	return mn;
}

SKVersionConstraintMode_M MVersionConstraint::getMode()
{
	SKVersionConstraintMode mode = ((SKVersionConstraint*)pImpl)->getMode();
	return (SKVersionConstraintMode_M) mode;
}

bool MVersionConstraint::matches(Int64 version)
{
	return  ((SKVersionConstraint*)pImpl)->matches(version);
}

bool MVersionConstraint::overlaps(MVersionConstraint ^ other)
{
	SKVersionConstraint * pVc = (SKVersionConstraint *) (other->getPImpl()->pVersionConstraint );
	return  ((SKVersionConstraint*)pImpl)->overlaps(pVc);
}

bool MVersionConstraint::equals(MVersionConstraint ^ other)
{
	SKVersionConstraint * pVc = (SKVersionConstraint *) (other->getPImpl()->pVersionConstraint );
	return  ((SKVersionConstraint*)pImpl)->equals(pVc);
}

String ^ MVersionConstraint::toString()
{
	std::string stdstr =  ((SKVersionConstraint*)pImpl)->toString();
	String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
	return str;
}

MVersionConstraint ^ MVersionConstraint::max(Int64 newMaxVal)
{
	((SKVersionConstraint*)pImpl)->max( newMaxVal );
	return this;
}

MVersionConstraint ^ MVersionConstraint::min(Int64 newMinVal)
{
	((SKVersionConstraint*)pImpl)->min( newMinVal );
	return this;
}

MVersionConstraint ^ MVersionConstraint::mode(SKVersionConstraintMode_M mode)
{
	((SKVersionConstraint*)pImpl)->mode( (SKVersionConstraintMode) mode );
	return this;
}

MVersionConstraint ^ MVersionConstraint::maxCreationTime(Int64 maxCreationTime)
{
	((SKVersionConstraint*)pImpl)->maxCreationTime( maxCreationTime );
	return this;
}


}