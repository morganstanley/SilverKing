#ifndef K_TYPES_H
#define K_TYPES_H

#include "k.h"
#include <stdexcept>
//#include "DHTLog.h"

template < typename KType, bool SKsList = false >
struct KBase {
    enum { BASE_TYPE = SKsList ? KType::type_code : -KType::type_code } ;
} ;

// function to get K type size
int get_type_size( int kType )
{
    switch (kType)
    {
        case KB:
        case KC:
        case KG: return 1;
        case KH: return 2;
        case KI:
        case KE:
        case KM:
        case KD:
        case KU:
        case KV:
        case KT: return 4;
        case KJ:
        case KF:
        case KZ: return 8;
        case KS: return sizeof(&kType); // pointer length
        case 0: 
            kp( "ERROR: Mixed lists are not yet supported" );
        default:
        krr( "Error: get_type_size - unsupported value type" ) ;
    }
    return -1;
}

struct KInt
{
    K    kptr_ ;
    
    enum { type_code = KI } ;
    
    KInt( K kptr ) : kptr_(kptr)
    { if ( kptr->t != -KI ) throw std::invalid_argument( "K Object is not of Integer type" ) ; }
    
    operator int () const { return kptr_->i ; }
} ;    

struct KSymbol
{
    K    kptr_ ;
    
    KSymbol( K kptr ) : kptr_(kptr)
    { 
       if ( kptr->t != -KS  && kptr->t != KC  ) 
       {
          throw std::invalid_argument( "K Object is not of Symbol type" ) ; 
       }
    }
    
    operator void* () const { return (void *)kptr_->s ; }
    size_t size() const { return  strlen(kptr_->s); }
} ;

struct KBytes
{     
    K    kptr_ ;
    
    KBytes( K kptr ) : kptr_(kptr)
    { 
        if ( kptr_->t == KC && kptr->n < 0 ) throw std::invalid_argument( "Bad Char array length" ) ; 
    }
    
    size_t size() const { 
        if (kptr_->t == KC) return (size_t)kptr_->n;
        if (kptr_->t == -KS) return  strlen(kptr_->s); // regular KS ????
        if (kptr_->t < 0) return get_type_size( -kptr_->t);
        if (kptr_->t > 0) return (size_t) kptr_->n * get_type_size(kptr_->t);
        
        krr( "Error: size - unsupported value type" ) ;
        return 0;
    } 
    operator char* () const { return (char *) kC(kptr_) ; }
} ;

struct KString : KBytes 
{
    KString( K kptr ) : KBytes(kptr) { }

    std::string string() const { return std::string( (char*)kC(kptr_), (size_t)(kptr_->n) ) ; }
} ;

struct KDict 
{
    K    kptr_ ;
    
    KDict( K kptr ) : kptr_(kptr)
    { 
        if ( kptr->t != XD ) 
        {
            throw std::invalid_argument( "K Object is not of Dict type" ) ; 
        }
    }

    K keys() { return kK(kptr_)[0] ; }
    K values() { return kK(kptr_)[1] ; }
} ;

struct KLong
{
    K    kptr_ ;
    
    enum { type_code = KJ } ;
    
    KLong( K kptr ) : kptr_(kptr)
    { 
        if ( kptr->t != -KJ ) 
            throw std::invalid_argument( "K Object is not of Long type" ) ; 
    }
    
    operator void* () const { return (void *) kptr_->j ; }
    operator int64_t () const { return (int64_t) kptr_->j ; }
    operator int () const { 
        if(kptr_->j < INT_MAX && kptr_->j >INT_MIN )
            return (int) kptr_->j ; 
        else
            throw std::invalid_argument( "KLong int overflow" ) ;
    }
} ;    


#if 0
template < typename KItem >
struct KList : KBase<KItem, true> 
{ 
    K    kptr_ ;

    KList( kptr ) : kptr_(kptr) {}
    
    operator size_t() const { return (size_t)kptr_->n ; }
    
    K& operator []( size_t ii ) { return (kK(kptr_)[ii]) ; }
    
} ;
#endif

struct Kinteger
{
    K    kptr_ ;
    
    
    Kinteger( K kptr ) : kptr_(kptr)
    { 
        if ( kptr->t != -KJ && kptr->t != -KI ) 
            throw std::invalid_argument( "K Object is not of integer type" ) ; 
    }
    
    operator void* () const { return (void *) ( (kptr_->t == -KJ) ? kptr_->j : kptr_->i); }
    operator int64_t () const { return (int64_t) ( (kptr_->t == -KJ) ? kptr_->j : kptr_->i) ; }
    operator int () const { 
        if(kptr_->t == -KJ) {
            if(kptr_->j < INT_MAX && kptr_->j >INT_MIN )
                return (int) kptr_->j ; 
            else
                throw std::invalid_argument( "KLong int overflow" ) ;
        }
        else {
            return (int) kptr_->i ; 
        }
    }
} ;
#endif // K_TYPES_H
