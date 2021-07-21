/* Copyright 2021 Stanford University, NVIDIA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __MAX__
#define __MAX__(x,y) (((x) > (y)) ? (x) : (y))
#endif

#ifndef __MIN__
#define __MIN__(x,y) (((x) < (y)) ? (x) : (y))
#endif

namespace Legion {

  namespace TypePunning {
    // The tenth circle of hell is reserved for members of the C++ committee
    // that decided to deviate from C's support for type punning unions.
    // Add on to it the fact that it took them 9 fucking years to realize
    // that they needed std::atomic_ref and it's plain to see they are all
    // just a bunch of idiots that should never be allowed near a programming
    // language standard ever again. They've clearly never written lock-free
    // code in their lives.
    template<typename T>
    class Pointer {
    public:
      Pointer(void *p)
        : pointer(convert(p)) { }
      static inline T* convert(void *p)
      {
        T *ptr = NULL;
        static_assert(sizeof(ptr) == sizeof(p), "Fuck C++");
        memcpy(&ptr, &p, sizeof(p));
        return ptr;
      }
      inline operator T*(void) const { return (T*)pointer; }
      inline T operator*(void) const { return *pointer; }
      inline T operator[](size_t off) const { return pointer[off]; }
    private:
      volatile T *const pointer;
    };
    template<typename T, size_t ALIGNMENT = alignof(T)>
    class AlignedPointer {
    public:
      AlignedPointer(void *p)
        : off(align(p)), pointer(convert(p, off)) { }
      static inline T* convert(void *p, size_t off)
      {
        uint8_t *p1 = NULL;
        static_assert(sizeof(p1) == sizeof(p), "Fuck C++");
        memcpy(&p1, &p, sizeof(p));
        p1 = p1 - off;
        T *p2 = NULL;
        static_assert(sizeof(p1) == sizeof(p2), "Fuck C++");
        memcpy(&p2, &p1, sizeof(p1));
        return p2;
      }
      static inline size_t align(void *p)
      {
        long long ptr;
        static_assert(sizeof(ptr) == sizeof(p), "Fuck C++");
        memcpy(&ptr, &p, sizeof(ptr));
        return ptr % ALIGNMENT;
      }
      inline operator T*(void) const { return (T*)pointer; }
      inline T operator*(void) const { return *pointer; }
      inline size_t offset(void) const { return off; }
    private:
      size_t off;
      volatile T *const pointer;
    };
    template<typename T1, typename T2>
    class Alias {
    public:
      inline void load(const Pointer<T1> &pointer, size_t off = 0)
      {
        T1 value = pointer[off];
        memcpy(buffer, (void*)&value, sizeof(T1));
      }
      template<size_t ALIGNMENT>
      inline void load(const AlignedPointer<T1,ALIGNMENT> &pointer)
      {
        T1 value = *pointer;
        memcpy(buffer, (void*)&value, sizeof(T1));
      }
      inline T1 as_one(void) const
      {
        T1 result;
        memcpy((void*)&result, buffer, sizeof(result));
        return result;
      }
      inline T2 as_two(void) const
      {
        T2 result;
        memcpy((void*)&result, buffer, sizeof(result));
        return result;
      }
      inline Alias& operator=(T2 rhs)
      {
        memcpy(buffer, (void*)&rhs, sizeof(rhs));
        return *this;
      }
    private:
      // Make this one private so it is can never be called
      inline Alias& operator=(T1 rhs)
      {
        memcpy(buffer, (void*)&rhs, sizeof(rhs));
        return *this;
      }
      static_assert(sizeof(T1) == sizeof(T2), "Sizes must match");
      uint8_t buffer[sizeof(T1)];
    };
  }; // TypePunning

#ifdef __CUDACC__
  // We have these functions here because calling memcpy (per the
  // insistence of the idiots on the C++ standards committee) on the
  // GPU is a terrible idea since it will spill the data out of registers
  // and into local memory in order to do the memcpy. Hence we tell
  // the compiler exactly what we mean using bit operations and inline PTX.
  
  __device__ __forceinline__
  bool __uint2bool(unsigned int value, unsigned offset)
  {
    value = value >> (8*offset); 
    return ((value & 0xFF) != 0);
  }

  __device__ __forceinline__
  unsigned int __bool2uint(unsigned int previous, bool value, unsigned offset)
  {
    unsigned int next = value;
    next = next << (8*offset);
    unsigned int mask = 0xFF;
    mask = mask << (8*offset);
    previous = previous & (~mask);
    return previous | next;
  }

  __device__ __forceinline__
  uint8_t __uint2ubyte(unsigned int value, unsigned offset)
  {
    value = value >> (8*offset);
    return uint8_t(value & 0xFF);
  }

  __device__ __forceinline__
  unsigned int __ubyte2uint(unsigned int previous, uint8_t value, unsigned offset)
  {
    unsigned int next = value;
    next = next << (8*offset);
    unsigned int mask = 0xFF;
    mask = mask << (8*offset);
    previous = previous & (~mask);
    return previous | next;
  }

  __device__ __forceinline__
  int8_t __int2byte(int value, unsigned offset)
  {
    value = value >> (8*offset);
    return int8_t(value & 0xFF);
  }

  __device__ __forceinline__
  int __byte2int(int previous, int8_t value, unsigned offset)
  {
    int next = value;
    next = next << (8*offset);
    unsigned int mask = 0xFF;
    mask = mask << (8*offset);
    previous = previous & (~mask);
    return previous | next;
  }

  __device__ __forceinline__
  unsigned short int __short_as_ushort(short int value)
  {
    unsigned short int result;
    asm("mov.b16 %0, %1;" : "=h"(result) : "h"(value));
    return result;
  }

  __device__ __forceinline__
  short int __ushort_as_short(unsigned short int value)
  {
    short int result;
    asm("mov.b16 %0, %1;" : "=h"(result) : "h"(value));
    return result;
  }

  __device__ __forceinline__
  unsigned int __hiloushort2uint(unsigned short int hi, unsigned short int lo)
  {
    unsigned int result;
    asm("mov.b32 %0, {%1,%2};" : "=r"(result) : "h"(lo), "h"(hi));
    return result;
  }

  __device__ __forceinline__
  unsigned short int __uint2loushort(unsigned int value)
  {
    unsigned short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return lo + 0*hi;
  }

  __device__ __forceinline__
  unsigned short int __uint2hiushort(unsigned int value)
  {
    unsigned short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return hi + 0*lo;
  }

  __device__ __forceinline__
  unsigned int __hiloshort2uint(short int hi, short int lo)
  {
    unsigned int result;
    asm("mov.b32 %0, {%1,%2};" : "=r"(result) : "h"(lo), "h"(hi));
    return result;
  }

  __device__ __forceinline__
  short int __uint2loshort(unsigned int value)
  {
    short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return lo + 0*hi;
  }

  __device__ __forceinline__
  short int __uint2hishort(unsigned int value)
  {
    short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return hi + 0*lo;
  }

#ifdef LEGION_REDOP_HALF
  __device__ __forceinline__
  unsigned int __hilohalf2uint(__half hi, __half lo)
  {
    unsigned int result;
    asm("mov.b32 %0, {%1,%2};" : "=r"(result) : "h"(__half_as_short(lo)), "h"(__half_as_short(hi)));
    return result;
  }

  __device__ __forceinline__
  __half __uint2hihalf(unsigned int value)
  {
    short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return __short_as_half(hi) + __half(0)*__short_as_half(lo);
  }
  
  __device__ __forceinline__
  __half __uint2lohalf(unsigned int value)
  {
    short int lo, hi;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(lo), "=h"(hi) : "r"(value));
    return __short_as_half(lo) + __half(0)*__short_as_half(hi);
  }
#endif

  __device__ __forceinline__
  unsigned int __int_as_uint(int value)
  {
    unsigned int result;
    asm("mov.b32 %0, %1;" : "=r"(result) : "r"(value));
    return result;
  }

  __device__ __forceinline__
  int __uint_as_int(unsigned int value)
  {
    int result;
    asm("mov.b32 %0, %1;" : "=r"(result) : "r"(value));
    return result;
  }

  __device__ __forceinline__
  unsigned long long __longlong_as_ulonglong(long long value)
  {
    unsigned long long result;
    asm("mov.b64 %0, %1;" : "=l"(result) : "l"(value));
    return result;
  }

  __device__ __forceinline__
  long long __ulonglong_as_longlong(unsigned long long value)
  {
    long long result;
    asm("mov.b64 %0, %1;" : "=l"(result) : "l"(value));
    return result;
  }

  __device__ __forceinline__
  double __ulonglong_as_double(unsigned long long value)
  {
    double result;
    asm("mov.b64 %0, %1;" : "=d"(result) : "l"(value));
    return result;
  }

  __device__ __forceinline__
  unsigned long long __double_as_ulonglong(double value)
  {
    unsigned long long result;
    asm("mov.b64 %0, %1;" : "=l"(result) : "d"(value));
    return result;
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  __device__ __forceinline__
  unsigned int __complex_as_uint(complex<__half> value)
  {
    unsigned int result;
    unsigned short int real = __half_as_ushort(value.real());
    unsigned short int imag = __half_as_ushort(value.imag());
    asm("mov.b32 %0, {%1,%2};" : "=r"(result) : "h"(real), "h"(imag));
    return result;
  }

  __device__ __forceinline__
  complex<__half> __uint_as_complex(unsigned int value)
  {
    unsigned short int real, imag;
    asm("mov.b32 {%0,%1}, %2;" : "=h"(real), "=h"(imag) : "r"(value));
    return complex<__half>(__ushort_as_half(real), __ushort_as_half(imag));
  }
#endif // LEGION_REDOP_HALF
  __device__ __forceinline__
  unsigned long long __complex_as_ulonglong(complex<float> value)
  {
    unsigned long long result;
    asm("mov.b64 %0, {%1,%2};" : "=l"(result) : "f"(value.real()), "f"(value.imag()));
    return result;
  }

  __device__ __forceinline__
  complex<float> __ulonglong_as_complex(unsigned long long value)
  {
    float real, imag;
    asm("mov.b64 {%0,%1}, %2;" : "=f"(real), "=f"(imag) : "l"(value));
    return complex<float>(real, imag);
  }
#endif // LEGION_REDOP_COMPLEX
#endif // __CUDACC__

  template<> __CUDA_HD__ inline
  void SumReduction<bool>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs || rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<bool>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous || rhs;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval || rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() || rhs;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
                    oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<bool>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 || rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<bool>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous || rhs2;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval || rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() || rhs2;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous + rhs;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous + rhs2;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval += rhs;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval += rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval += rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval += rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval += rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&lhs, rhs);
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&rhs1, rhs2);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval += rhs;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous + rhs;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous + rhs2;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline 
  void SumReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval += rhs;
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval += rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval += rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval += rhs2;
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval += rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval += rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&lhs, rhs); 
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&rhs1, rhs2);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicAdd((unsigned long long*)&lhs, (unsigned long long)rhs);
#else
    __sync_fetch_and_add(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicAdd((unsigned long long*)&rhs1, (unsigned long long)rhs2);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void SumReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs + rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    atomicAdd(&lhs,rhs);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = newval + rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = newval + rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) + float(rhs));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 + rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    atomicAdd(&rhs1, rhs2);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = newval + rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = newval + rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) + float(rhs2));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void SumReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&lhs, rhs);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicAdd(&rhs1, rhs2);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 600
    atomicAdd(&lhs, rhs);
#else
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval += rhs;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(oldval)));
    } while (oldval != newval);
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 600
    atomicAdd(&rhs1, rhs2);
#else
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(oldval)));
    } while (oldval != newval);
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void SumReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval += rhs;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void SumReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval += rhs;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() + rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<double> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs += rhs;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<double> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 600
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    double *lptr = (double*)&lhs;
    atomicAdd(lptr, rhs.real());
    atomicAdd(lptr+1, rhs.imag());
#else
    double newval = lhs.real(), oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval += rhs.real();
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);   
    newval = lhs.imag();
    do {
      oldval = newval;
      newval += rhs.imag();
      newval = __ulonglong_as_double(atomicCAS(ptr+1,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    for (unsigned i = 0; i < 2; ++i) {
      TypePunning::Alias<int64_t,double> oldval, newval;
      do {
        oldval.load(pointer, i);
        newval = oldval.as_two() + ((i == 0) ? rhs.real() : rhs.imag());
      } while (!__sync_bool_compare_and_swap(((int64_t*)pointer) + i,
                            oldval.as_one(), newval.as_one()));
    }
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<double> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void SumReduction<complex<double> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 600
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    double *lptr = (double*)&rhs1;
    atomicAdd(lptr, rhs2.real());
    atomicAdd(lptr+1, rhs2.imag());
#else
    double newval = rhs1.real(), oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval += rhs2.real();
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);   
    newval = rhs1.imag();
    do {
      oldval = newval;
      newval += rhs2.imag();
      newval = __ulonglong_as_double(atomicCAS(ptr+1,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval + rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    for (unsigned i = 0; i < 2; ++i) {
      TypePunning::Alias<int64_t,double> oldval, newval;
      do {
        oldval.load(pointer, i);
        newval = oldval.as_two() + ((i == 0) ? rhs2.real() : rhs2.imag());
      } while (!__sync_bool_compare_and_swap(((int64_t*)pointer) + i,
                            oldval.as_one(), newval.as_one()));
    }
#endif
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void DiffReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous - rhs;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous - rhs2;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval -= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval -= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 += rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval -= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval -= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicSub(&lhs, rhs);
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicSub(&rhs1, rhs2);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous - rhs;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous - rhs2;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline 
  void DiffReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval -= rhs;
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval -= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval -= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval -= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval -= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicSub(&lhs, rhs); 
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicSub(&rhs1, rhs2);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64 bit int atomic yet
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval = *target;
    do {
      oldval = newval;
      newval -= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_sub(&lhs, rhs);
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64 bit int atomic yet
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval = *target;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
    __sync_fetch_and_add(&rhs1, rhs2);
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void DiffReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs - rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = newval - rhs;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = newval - rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = newval - rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) - float(rhs));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 - rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = newval - rhs2;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = newval - rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = newval - rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) - float(rhs2));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void DiffReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void DiffReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs -= rhs;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval -= rhs;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 -= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DiffReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval -= rhs2;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval - rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() - rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void ProdReduction<bool>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs && rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<bool>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous && rhs;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval && rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() && rhs;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
                    oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<bool>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 && rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<bool>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous && rhs2;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval || rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() || rhs2;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous * rhs;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous * rhs2;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval *= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval *= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval *= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval *= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&lhs;
    int oldval, newval = lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&rhs1;
    int oldval, newval = rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous * rhs;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous * rhs2;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline 
  void ProdReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval *= rhs;
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval *= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval *= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval *= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval *= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&lhs;
    unsigned oldval, newval = lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&rhs1;
    unsigned oldval, newval = rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval = lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval * rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval = rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval * rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void ProdReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs * rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = newval * rhs;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = newval * rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = newval * rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) * float(rhs));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 * rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = newval * rhs2;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = newval * rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = newval * rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) * float(rhs2));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void ProdReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void ProdReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs *= rhs;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval *= rhs;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void ProdReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval *= rhs2;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval * rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() * rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void DivReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous / rhs;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = previous / rhs2;
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval /= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval /= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval /= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval /= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&lhs;
    int oldval, newval = lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&rhs1;
    int oldval, newval = rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous / rhs;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = previous / rhs2;
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline 
  void DivReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval /= rhs;
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval /= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval /= rhs;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval /= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval /= rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&lhs;
    unsigned oldval, newval = lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&rhs1;
    unsigned oldval, newval = rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval = lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval / rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval = rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval / rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void DivReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs / rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = newval / rhs;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = newval / rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = newval / rhs;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) / float(rhs));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 * rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = newval / rhs2;
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = newval / rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = newval / rhs2;
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(
          __convert_halfint_to_float(next[offset]) / float(rhs2));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void DivReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void DivReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs2;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void DivReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs /= rhs;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval /= rhs;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval / rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 *= rhs2;
  }

  template<> __CUDA_HD__ inline
  void DivReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval /= rhs2;
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() / rhs2;
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void MaxReduction<bool>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (!lhs && rhs)
      lhs = true;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<bool>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous || rhs;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval || rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() || rhs;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
                    oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<bool>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (!rhs1 && rhs2)
      rhs1 = true;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<bool>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous || rhs2;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval || rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() || rhs2;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = __MAX__(previous, rhs);
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = __MAX__(previous, rhs2);
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicMax(&lhs, rhs);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicMax(&rhs1, rhs2);  
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = __MAX__(previous, rhs);
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = __MAX__(previous, rhs2);
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline 
  void MaxReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicMax(&lhs, rhs);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicMax(&rhs1, rhs2); 
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicMax((unsigned long long*)&lhs, rhs); 
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicMax((unsigned long long*)&rhs1, rhs2); 
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MAX__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void MaxReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(__MAX__(
          __convert_halfint_to_float(next[offset]), float(rhs)));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MAX__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(__MAX__(
          __convert_halfint_to_float(next[offset]), float(rhs2)));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void MaxReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void MaxReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs > lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs);
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 > rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MaxReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval = __MAX__(newval, rhs2);
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MAX__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MAX__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void MinReduction<bool>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (lhs && !rhs)
      lhs = false;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<bool>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous && rhs;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval && rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() && rhs;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
                    oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<bool>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs1 && !rhs2)
      rhs1 = false;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<bool>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2bool(newval, offset);
      RHS next = previous && rhs2;
      oldval = newval;
      newval = __bool2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval && rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() && rhs2;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = __MIN__(previous, rhs);
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(int);
    const uintptr_t aligned = unaligned - offset;
    int *ptr = reinterpret_cast<int*>(aligned);
    int newval = *ptr, oldval;
    do {
      RHS previous = __int2byte(newval, offset);
      RHS next = __MIN__(previous, rhs2);
      oldval = newval;
      newval = __byte2int(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __ushort_as_short(atomicCAS(ptr,
            __short_as_ushort(oldval), __short_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(other, oldval), __hiloshort2uint(other, newval));
        newval = __uint2loshort(result);
        other = __uint2hishort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloshort2uint(oldval, other), __hiloshort2uint(newval, other));
        other = __uint2loshort(result);
        newval = __uint2hishort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicMin(&lhs, rhs);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicMin(&rhs1, rhs2);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // Apparently there is no signed 64bit int atomic yet
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long int *ptr = (unsigned long long int*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __ulonglong_as_longlong(atomicCAS(ptr,
            __longlong_as_ulonglong(oldval), __longlong_as_ulonglong(oldval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = __MIN__(previous, rhs);
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    unsigned int newval = *ptr, oldval;
    do {
      RHS previous = __uint2ubyte(newval, offset);
      RHS next = __MIN__(previous, rhs2);
      oldval = newval;
      newval = __ubyte2uint(newval, next, offset);
      newval = atomicCAS(ptr, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline 
  void MinReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = atomicCAS(&lhs, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = atomicCAS(&rhs1, oldval, newval);
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(other, oldval), __hiloushort2uint(other, newval));
        newval = __uint2loushort(result);
        other = __uint2hiushort(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hiloushort2uint(oldval, other), __hiloushort2uint(newval, other));
        other = __uint2loushort(result);
        newval = __uint2hiushort(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    atomicMin(&lhs, rhs); 
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    atomicMin(&rhs1, rhs2); 
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // apparently there is no 64-bit atomic min
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval = lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // apparently there is no 64-bit atomic min
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval = rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = __MIN__(oldval, rhs2);
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void MinReduction<__half>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<__half>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&lhs);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = lhs, oldval, other;
    if (offset == 0) {
      other = *((&lhs)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&lhs)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t> pointer((void*)&lhs);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(__MIN__(
          __convert_halfint_to_float(next[offset]), float(rhs)));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer, 
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<__half>::fold<true>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    if (rhs2 < rhs1)
      rhs1 = rhs2;
#else
    if ((*reinterpret_cast<uint16_t*>(&rhs2)) < (*reinterpret_cast<uint16_t*>(&rhs1)))
      rhs1 = rhs2;
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<__half>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ >= 700
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned short int *ptr = (unsigned short int*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __ushort_as_half(atomicCAS(ptr,
            __half_as_ushort(oldval), __half_as_ushort(oldval)));
    } while (oldval != newval);
#else
    // 16-bit atomics are not supported prior to volta
    // 32-bit GPU atomics need 4 byte alignment
    const uintptr_t unaligned = reinterpret_cast<uintptr_t>(&rhs1);
    const unsigned offset = unaligned % sizeof(unsigned int);
    const uintptr_t aligned = unaligned - offset;
    unsigned int *ptr = reinterpret_cast<unsigned int*>(aligned);
    RHS newval = rhs1, oldval, other;
    if (offset == 0) {
      other = *((&rhs1)+1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(other, oldval), __hilohalf2uint(other, newval));
        newval = __uint2lohalf(result);
        other = __uint2hihalf(result);
      } while (oldval != newval);
    } else {
      other = *((&rhs1)-1);
      do {
        oldval = newval;
        newval = __MIN__(newval, rhs2);
        const unsigned int result = atomicCAS(ptr,
            __hilohalf2uint(oldval, other), __hilohalf2uint(newval, other));
        other = __uint2lohalf(result);
        newval = __uint2hihalf(result);
      } while (oldval != newval);
    }
#endif
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap
    TypePunning::Alias<int32_t,std::array<short,2> > oldval, newval;
    TypePunning::AlignedPointer<int32_t,alignof(int32_t)> pointer((void*)&rhs1);
    const unsigned offset = pointer.offset() / sizeof(__half);
    do {
      oldval.load(pointer);
      std::array<short,2> next = oldval.as_two();
      next[offset] = __convert_float_to_halfint(__MIN__(
          __convert_halfint_to_float(next[offset]), float(rhs2)));
      newval = next;
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void MinReduction<float>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<float>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<float>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<float>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    int *ptr = (int*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __int_as_float(atomicCAS(ptr,
            __float_as_int(oldval), __float_as_int(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int32_t,float> oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<double>::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<double>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<double>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<double>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __ulonglong_as_double(atomicCAS(ptr,
            __double_as_ulonglong(oldval), __double_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic floating point operations so use compare and swap 
    TypePunning::Alias<int64_t,double> oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

#ifdef LEGION_REDOP_COMPLEX
#ifdef LEGION_REDOP_HALF
  template<> __CUDA_HD__ inline
  void MinReduction<complex<__half> >::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<__half> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<__half> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<__half> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned int *ptr = (unsigned int*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __uint_as_complex(atomicCAS(ptr,
            __complex_as_uint(oldval), __complex_as_uint(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int32_t,complex<__half> > oldval, newval;
    TypePunning::Pointer<int32_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int32_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }
#endif // LEGION_REDOP_HALF

  template<> __CUDA_HD__ inline
  void MinReduction<complex<float> >::apply<true>(LHS &lhs, RHS rhs)
  {
    if (rhs < lhs)
      lhs = rhs;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<float> >::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    RHS newval = lhs, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&lhs;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs);
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<float> >::fold<true>(RHS &rhs1, RHS rhs2)
  {
    if (rhs2 < rhs1)
      rhs1 = rhs2;
  }

  template<> __CUDA_HD__ inline
  void MinReduction<complex<float> >::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    RHS newval = rhs1, oldval;
    // Type punning like this is illegal in C++ but the
    // CUDA manual has an example just like it so fuck it
    unsigned long long *ptr = (unsigned long long*)&rhs1;
    do {
      oldval = newval;
      newval = __MIN__(newval, rhs2);
      newval = __ulonglong_as_complex(atomicCAS(ptr,
            __complex_as_ulonglong(oldval), __complex_as_ulonglong(newval)));
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = __MIN__(oldval, rhs2);
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    TypePunning::Alias<int64_t,complex<float> > oldval, newval;
    TypePunning::Pointer<int64_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = __MIN__(oldval.as_two(), rhs2);
    } while (!__sync_bool_compare_and_swap((int64_t*)pointer,
                      oldval.as_one(), newval.as_one()));
#endif
#endif
  }
#endif // LEGION_REDOP_COMPLEX

  template<> __CUDA_HD__ inline
  void OrReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F |= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F |= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x |= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y |= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x |= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y |= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&lhs;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&rhs1;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed |= rhs;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed |= rhs2;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F |= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F |= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline 
  void OrReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x |= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y |= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x |= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y |= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&lhs;
    unsigned oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&rhs1;
    unsigned oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs |= rhs;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval | rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 |= rhs2;
  }

  template<> __CUDA_HD__ inline
  void OrReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval |= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval | rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval | rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F &= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F &= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x &= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y &= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x &= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y &= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&lhs;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval &= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&rhs1;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval &= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed &= rhs;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed &= rhs2;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F &= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F &= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline 
  void AndReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x &= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y &= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x &= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y &= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&lhs;
    unsigned oldval, newval;
    newval = *(volatile unsigned*)target;
    do {
      oldval = newval;
      newval &= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&rhs1;
    unsigned oldval, newval;
    newval = *(volatile unsigned*)target;
    do {
      oldval = newval;
      newval &= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs &= rhs;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval;
    newval = *(volatile unsigned long long*)target;
    do {
      oldval = newval;
      newval &= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval & rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 &= rhs2;
  }

  template<> __CUDA_HD__ inline
  void AndReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval;
    newval = *(volatile unsigned long long*)target;
    do {
      oldval = newval;
      newval &= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval & rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval & rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<bool>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs = lhs ^ rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<bool>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    const unsigned offset = ((unsigned long long)ptr) % 4;
    int *target = (int *)(ptr - offset);
    union { int as_int; bool as_bool[4]; } oldval, newval;
    newval.as_int = *(volatile int*)target;
    do {
      oldval.as_int = newval.as_int;
      newval.as_bool[offset] = newval.as_bool[offset] ^ rhs;
      newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
    } while (oldval.as_bool[offset] != newval.as_bool[offset]);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval != rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&lhs);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() != rhs;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
                    oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<bool>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 = rhs1 ^ rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<bool>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    const unsigned offset = ((unsigned long long)ptr) % 4;
    int *target = (int *)(ptr - offset);
    union { int as_int; bool as_bool[4]; } oldval, newval;
    newval.as_int = *(volatile int*)target;
    do {
      oldval.as_int = newval.as_int;
      newval.as_bool[offset] = newval.as_bool[offset] ^ rhs2;
      newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
    } while (oldval.as_bool[offset] != newval.as_bool[offset]);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval != rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    TypePunning::Alias<int8_t,bool> oldval, newval;
    TypePunning::Pointer<int8_t> pointer((void*)&rhs1);
    do {
      oldval.load(pointer);
      newval = oldval.as_two() != rhs2;
    } while (!__sync_bool_compare_and_swap((int8_t*)pointer,
          oldval.as_one(), newval.as_one()));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F ^= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(int8_t)*IDX);                     \
        union { int as_int; char4 as_char; } oldval, newval;               \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F ^= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x ^= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y ^= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x ^= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(short));
      union { int as_int; short2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y ^= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&lhs;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval ^= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    int *target = (int *)&rhs1;
    int oldval, newval;
    newval = *(volatile int*)target;
    do {
      oldval = newval;
      newval ^= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed ^= rhs;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<int64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    union { long long as_signed; unsigned long long as_unsigned; } oldval, newval;
    newval.as_unsigned = *(volatile unsigned long long*)target;
    do {
      oldval.as_signed = newval.as_signed;
      newval.as_signed ^= rhs2;
      newval.as_unsigned = atomicCAS(target, oldval.as_unsigned, newval.as_unsigned);
    } while (oldval.as_signed != newval.as_signed);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint8_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint8_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F ^= rhs;                                         \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint8_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint8_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
#define CASE(F, IDX)                                                       \
      case IDX : {                                                         \
        int *target = (int *)(ptr-sizeof(uint8_t)*IDX);                    \
        union { int as_int; uchar4 as_char; } oldval, newval;              \
        newval.as_int = *(volatile int*)target;                            \
        do {                                                               \
          oldval.as_int = newval.as_int;                                   \
          newval.as_char.F ^= rhs2;                                        \
          newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int); \
        } while (oldval.as_int != newval.as_int);                          \
        break;                                                             \
      }                                                                    \

    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    switch (((unsigned long long)ptr) % 4) {
      CASE(x, 0)
      CASE(y, 1)
      CASE(z, 2)
      CASE(w, 3)
      default :{ assert(false); break; }
    }
#undef CASE
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint16_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline 
  void XorReduction<uint16_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&lhs;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x ^= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y ^= rhs;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint16_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint16_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    // GPU atomics need 4 byte alignment
    char *ptr = (char*)&rhs1;
    if ((((unsigned long long)ptr) % 4) == 0) {
      // Aligned case
      int *target = (int *)ptr;
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.x ^= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    } else {
      // Unaligned case
      int *target = (int *)(ptr-sizeof(unsigned short));
      union { int as_int; ushort2 as_short; } oldval, newval;
      newval.as_int = *(volatile int*)target;
      do {
        oldval.as_int = newval.as_int;
        newval.as_short.y ^= rhs2;
        newval.as_int = atomicCAS(target, oldval.as_int, newval.as_int);
      } while (oldval.as_int != newval.as_int);
    }
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint32_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint32_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&lhs;
    unsigned oldval, newval;
    newval = *(volatile unsigned*)target;
    do {
      oldval = newval;
      newval ^= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint32_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint32_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned *target = (unsigned *)&rhs1;
    unsigned oldval, newval;
    newval = *(volatile unsigned*)target;
    do {
      oldval = newval;
      newval ^= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint64_t>::apply<true>(LHS &lhs, RHS rhs)
  {
    lhs ^= rhs;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint64_t>::apply<false>(LHS &lhs, RHS rhs)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&lhs;
    unsigned long long oldval, newval;
    newval = *(volatile unsigned long long*)target;
    do {
      oldval = newval;
      newval ^= rhs;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(lhs);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&lhs);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint64_t>::fold<true>(RHS &rhs1, RHS rhs2)
  {
    rhs1 ^= rhs2;
  }

  template<> __CUDA_HD__ inline
  void XorReduction<uint64_t>::fold<false>(RHS &rhs1, RHS rhs2)
  {
#ifdef __CUDA_ARCH__
    unsigned long long *target = (unsigned long long *)&rhs1;
    unsigned long long oldval, newval;
    newval = *(volatile unsigned long long*)target;
    do {
      oldval = newval;
      newval ^= rhs2;
      newval = atomicCAS(target, oldval, newval);
    } while (oldval != newval);
#else
#if __cplusplus >= 202002L 
    std::atomic_ref<RHS> atomic(rhs1);
    RHS oldval = atomic.load();
    RHS newval;
    do {
      newval = oldval ^ rhs2;
    } while (atomic.compare_exchange_weak(oldval, newval));
#else
    // No atomic logical operations so use compare and swap
    RHS oldval, newval;
    TypePunning::Pointer<RHS> pointer((void*)&rhs1);
    do {
      oldval = *pointer;
      newval = oldval ^ rhs2;
    } while (!__sync_bool_compare_and_swap((RHS*)pointer, oldval, newval));
#endif
#endif
  }

}; // namespace Legion

#undef __MAX__
#undef __MIN__

