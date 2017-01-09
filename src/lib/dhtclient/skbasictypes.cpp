
#include "skbasictypes.h"
#include "skcommon.h"
#include <stdlib.h> 
#include <string.h>

#ifdef _MSC_VER
#include <process.h>
#include <Windows.h>
#include "dbghelp.h"
#else
#include <cxxabi.h>
#include <execinfo.h>
#include <ucontext.h>
#include <unistd.h>
#endif

//#ifdef __cplusplus
//extern "C" {
//#endif


SKVal* sk_create_val()
{
  SKVal *pVal = (SKVal*)skMemAlloc(1, sizeof(SKVal), __FILE__, __LINE__);
  if (!pVal) return 0;
  pVal->m_rc = SKOperationState::SUCCEEDED;
  pVal->m_len = 0;
  pVal->m_pVal = 0;

  return pVal;
}

void sk_clear_val(SKVal* pVal_)
{
	if (pVal_) {
		if (pVal_->m_pVal != NULL && pVal_->m_pVal != SK_NULL_VALUE) {
			skMemFree(&(pVal_->m_pVal), __FILE__, __LINE__);
		}
		pVal_->m_pVal = 0;
		pVal_->m_rc = SKOperationState::SUCCEEDED;
		pVal_->m_len = 0;
	}
}

void sk_destroy_val(SKVal** pVal_)
{
  if (!pVal_) return;

  if (*pVal_) {
	if ((*pVal_)->m_pVal && (*pVal_)->m_pVal != SK_NULL_VALUE) {
	  skMemFree(&((*pVal_)->m_pVal), __FILE__, __LINE__);
	  (*pVal_)->m_pVal = 0;
	}
    skMemFree((void**)pVal_, __FILE__, __LINE__);
  }

  *pVal_ = 0;
}

void sk_move_val(SKVal *pSrc_, SKVal *pDst_)
{
  if (pSrc_)
  {
    if (pDst_)
    {
	  if (pDst_->m_pVal) {
		skMemFree(&(pDst_->m_pVal), __FILE__, __LINE__);
	  }
      pDst_->m_rc = pSrc_->m_rc;
      pDst_->m_len = pSrc_->m_len;
      pDst_->m_pVal = pSrc_->m_pVal;
    }
    else
    {
      skMemFree(&(pSrc_->m_pVal), __FILE__, __LINE__);
    }

    pSrc_->m_rc = SKOperationState::SUCCEEDED;
    pSrc_->m_len = 0;
    pSrc_->m_pVal = 0;
  }
}

void sk_set_val(SKVal *pVal_, int size_, void *src_)
{
  if (size_ >= 0)
  {
    if (pVal_->m_pVal)
    {
      skMemFree(&(pVal_->m_pVal), __FILE__, __LINE__);
      pVal_->m_pVal = 0;
    }
    pVal_->m_len = 0;

    if (size_ > 0)
    {
      pVal_->m_len = size_;
      pVal_->m_pVal = skMemAlloc(1, size_, __FILE__, __LINE__);
      memcpy(pVal_->m_pVal, src_, size_);
    }
  }
}

void sk_set_val_zero_copy(SKVal *pVal_, int size_, void *src_)
{
  if (size_ >= 0)
  {
    if (pVal_->m_pVal)
    {
      skMemFree(&(pVal_->m_pVal), __FILE__, __LINE__);
      pVal_->m_pVal = 0;
    }
    pVal_->m_len = 0;

    if (size_ > 0)
    {
      pVal_->m_len = size_;
      pVal_->m_pVal = src_;
    }
  }
}

bool dht_is_valid_val(SKVal *pVal_)
{
  if (pVal_)
    {
      if (pVal_->m_rc == SKOperationState::SUCCEEDED && pVal_->m_pVal)
          return true;
    }

  return false;
}

bool sk_is_valid_existing_val(SKVal *pVal_)
{
  if (pVal_)
    {
      if (pVal_->m_rc == SKOperationState::SUCCEEDED && pVal_->m_pVal)
        return true;
    }

  return false;
}

/*
SKValMetaData* sk_create_value_meta()
{
  SKValMetaData *p = (SKValMetaData*)skMemAlloc(sizeof(SKValMetaData), __FILE__, __LINE__);
  if (!p) return 0;

  p->userData.m_rc = SKOperationState::SUCCEEDED;
  p->userData.m_len = 0;
  p->userData.m_pVal = 0;

  return p;
}

void sk_destroy_value_meta(SKValMetaData** pValMeta_)
{
  if (!pValMeta_) return;

  if (*pValMeta_)
  {
    if ((*pValMeta_)->userData.m_pVal)
      skMemFree(&((*pValMeta_)->userData.m_pVal), __FILE__, __LINE__);
    skMemFree((void**)pValMeta_, __FILE__, __LINE__);
  }

  *pValMeta_ = 0;
}
*/

void skFatalError(const char *msg, const char *file, int line) {
    printf("FATAL ERROR: %s\t%s line:%d\n", msg, file, line);
    fflush(0);
	exit(-1);
}

void *skMemAlloc(size_t nmemb, size_t size, const char *file, int line) {
	void * ptr = NULL;
     //if(rand() > RAND_MAX/1000 ){  // this is to test the OOM behavior 
        ptr = calloc(nmemb, size);
    //}
	if (ptr == NULL) {
		skFatalError("out of memory", file, line);
	}
	return ptr;
}
void *skMemDup(const void *source, int size, const char *file, int line) {
	void * dest = skMemAlloc(size, 1, file, line);
    memcpy(dest, source, size);
	return dest;
}

char *skStrDup(const char *source, const char *file, int line) {
	return (char *)skMemDup(source, strlen(source) + 1, file, line);
}


void skMemFree(void **ptr, const char *file, int line) {

    //if(rand() < RAND_MAX/1000 ){  // this is to test the OOM behavior 
    //    ptr = NULL;
    //}

	if (ptr == NULL) {
		skFatalError("skMemFree called with NULL ptr", file, line);
	}
	if (*ptr == NULL) {
		skFatalError("skMemFree called with NULL *ptr", file, line);
	}
	free(*ptr);
	*ptr = NULL;
}

void print_stacktrace(const char* source, FILE *out, const unsigned int max_frames )
{
#ifndef  _MSC_VER
    char execname[1024];
    char buf[1024];
    pid_t pid;
    int retval;

    /* Get the pid */
    pid = getpid();

    snprintf(execname, sizeof(execname), "/proc/%i/exe", pid);

    /* get binary's name from symlink /proc/<pid>/exe  */
    retval = readlink(execname, buf, 1024);
    buf[retval] = 0;  //terminate str

    fprintf(out, "stack trace (%s) for process %s (pid:%d):\n", source, buf, pid);

    // array for stack trace address data
    void* addresslist[max_frames+1];

    // get stack addresses
    int addrlen = backtrace(addresslist, sizeof(addresslist) / sizeof(void*));

    if (addrlen == 0) {
        fprintf(out, "  \n");
        return;
    }

    // this yields strings containing "filename(function+address)",
    char** symbollist = backtrace_symbols(addresslist, addrlen);

    // allocate string which will be filled with the demangled function name
    size_t funcnamelen = 256;
    char* funcname = (char*)malloc(funcnamelen);

    // walk over the returned symbols. except first two (this function and handler)
    for (int i = 2; i < addrlen; i++)
    {
		char *func_name = 0, *addr_offset = 0, *addr_end = 0;

		// parse out _mangled_ function and hexaddroffset from  "./module(function+hexaddroffset) [hexaddr]"
		for (char *p = symbollist[i]; *p; ++p)
		{
			if (*p == '(')
			func_name = p;
			else if (*p == '+')
			addr_offset = p;
			else if (*p == ')' && addr_offset) {
			addr_end = p;
			break;
			}
		}

		if (func_name && addr_offset && addr_end && func_name < addr_offset)  //sanity check
		{
			*func_name++ = '\0';
			*addr_offset++ = '\0';
			*addr_end = '\0';

			// demangle name with __cxa_demangle(), demangled name in funcname
			int errCode;
			char* retStr = abi::__cxa_demangle(func_name, funcname, &funcnamelen, &errCode);
			if (errCode) {
				// demangling failed. print fn as C name
				fprintf(out, "   pid:%d %s : %s()+%s\n",
					pid, symbollist[i], func_name, addr_offset);
			}
			else {
				funcname = retStr; // possibly reallocated
				fprintf(out, "   pid:%d %s : %s+%s\n",
					pid, symbollist[i], funcname, addr_offset);
			}
		}
		else
		{
			// we could not parse out fn and addr from symbollist[i] , print it as is
			fprintf(out, "   pid:%d %s: non-parsed frame\n", pid, symbollist[i]);
		}
    }

	//these must be freed by caller
    free(funcname);
    free(symbollist); 

    fprintf(out, "stack trace END pid:%d\n", pid);
#else
	void **stack = (void**)malloc(max_frames*sizeof(void*));
	HANDLE process = GetCurrentProcess();
	SymInitialize(process, NULL, TRUE);
	WORD numberOfFrames = CaptureStackBackTrace(0, max_frames, stack, NULL);
	SYMBOL_INFO *symbol = (SYMBOL_INFO *)malloc(sizeof(SYMBOL_INFO));
	symbol->MaxNameLen = 1024;
	symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
	IMAGEHLP_LINE *line = (IMAGEHLP_LINE *)malloc(sizeof(IMAGEHLP_LINE));
	line->SizeOfStruct = sizeof(IMAGEHLP_LINE);
    fprintf(out, "stack trace (%s) for PID:%d:\n",  source, (int)_getpid() );
	printf("Caught exception ");
	for (int i = 0; i < numberOfFrames; i++)
	{
		SymFromAddr(process, (DWORD64)(stack[i]), NULL, symbol);
		DWORD dwDisplacement;
		SymGetLineFromAddr(process, (DWORD)(stack[i]), &dwDisplacement, line);
		fprintf(out, "at %s in %s, address 0x%0X\n", symbol->Name, line->FileName, symbol->Address);
	}
	free(line);
	free(symbol);
	free(stack);
#endif
}


//#ifdef __cplusplus
//}
//#endif
