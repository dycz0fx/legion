/* Copyright 2017 Stanford University, NVIDIA Corporation
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

#ifndef __LEGION_REPLICATION_H__
#define __LEGION_REPLICATION_H__

#include "legion_ops.h"
#include "legion_tasks.h"

namespace Legion {
  namespace Internal {

    /**
     * \class IndexSpaceReduction
     * A class for performing reductions of index spaces
     */
    class IndexSpaceReduction {
    public:
      typedef IndexSpace LHS;
      typedef IndexSpace RHS;
      static const IndexSpace identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert((lhs.exists() && !rhs.exists()) ||
               (!lhs.exists() && rhs.exists()) ||
               (lhs.exists() && (lhs == rhs)));
#endif
        if (rhs.exists())
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert((rhs1.exists() && !rhs2.exists()) ||
               (!rhs1.exists() && rhs2.exists()) ||
               (rhs1.exists() && (rhs1 == rhs2)));
#endif
        if (rhs2.exists())
          rhs1 = rhs2;
      }
    };

    /**
     * \class IndexPartitionReduction
     * A class for performing reductions of index partition IDs
     */
    class IndexPartitionReduction {
    public:
      typedef IndexPartitionID LHS;
      typedef IndexPartitionID RHS;
      static const IndexPartitionID identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert(((lhs != 0) && (rhs == 0)) ||
               ((lhs == 0) && (rhs != 0)) ||
               ((lhs != 0) && (lhs == rhs)));
#endif
        if (rhs != 0)
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert(((rhs1 != 0) && (rhs2 == 0)) ||
               ((rhs1 == 0) && (rhs2 != 0)) ||
               ((rhs1 != 0) && (rhs1 == rhs2)));
#endif
        if (rhs2 != 0)
          rhs1 = rhs2;
      }
    };

    /**
     * \class FieldSpaceReduction
     * A class for performing reductions of field spaces
     */
    class FieldSpaceReduction {
    public:
      typedef FieldSpace LHS;
      typedef FieldSpace RHS;
      static const FieldSpace identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert((lhs.exists() && !rhs.exists()) ||
               (!lhs.exists() && rhs.exists()) ||
               (lhs.exists() && (lhs == rhs)));
#endif
        if (rhs.exists())
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert((rhs1.exists() && !rhs2.exists()) ||
               (!rhs1.exists() && rhs2.exists()) ||
               (rhs1.exists() && (rhs1 == rhs2)));
#endif
        if (rhs2.exists())
          rhs1 = rhs2;
      }
    };

    /**
     * \class LogicalRegionReduction
     * A class for performing reductions of region tree IDs
     */
    class LogicalRegionReduction {
    public:
      typedef RegionTreeID LHS;
      typedef RegionTreeID RHS;
      static const RegionTreeID identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert(((lhs != 0) && (rhs == 0)) ||
               ((lhs == 0) && (rhs != 0)) ||
               ((lhs != 0) && (lhs == rhs)));
#endif
        if (rhs != 0)
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert(((rhs1 != 0) && (rhs2 == 0)) ||
               ((rhs1 == 0) && (rhs2 != 0)) ||
               ((rhs1 != 0) && (rhs1 == rhs2)));
#endif
        if (rhs2 != 0)
          rhs1 = rhs2;
      }
    };

    /**
     * \class FieldReduction
     * A class for performing reductions of field IDs
     */
    class FieldReduction {
    public:
      typedef FieldID LHS;
      typedef FieldID RHS;
      static const FieldID identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert(((lhs != 0) && (rhs == 0)) ||
               ((lhs == 0) && (rhs != 0)) ||
               ((lhs != 0) && (lhs == rhs)));
#endif
        if (rhs != 0)
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert(((rhs1 != 0) && (rhs2 == 0)) ||
               ((rhs1 == 0) && (rhs2 != 0)) ||
               ((rhs1 != 0) && (rhs1 == rhs2)));
#endif
        if (rhs2 != 0)
          rhs1 = rhs2;
      }
    };

#ifdef DEBUG_LEGION
    /**
     * \class ShardingReduction
     * A class for performing reductions of ShardingIDs
     * down to a single ShardingID. This is used in debug
     * mode to determine if the mappers across all shards
     * chose the same sharding ID for a given operation.
     * This is only used in debug mode
     */
    class ShardingReduction {
    public:
      typedef ShardingID LHS;
      typedef ShardingID RHS;
      static const ShardingID identity;

      template<bool EXCLUSIVE>
      static inline void apply(LHS &lhs, RHS rhs)
      {
#ifdef DEBUG_LEGION
        assert((lhs < UINT_MAX) || (rhs < UINT_MAX));
#endif
        if (lhs == UINT_MAX)
          lhs = rhs;
      }

      template<bool EXCLUSIVE>
      static inline void fold(RHS &rhs1, RHS rhs2)
      {
#ifdef DEBUG_LEGION
        assert((rhs1 < UINT_MAX) || (rhs2 < UINT_MAX));
#endif
        if (rhs1 == UINT_MAX)
          rhs1 = rhs2;
      }
    };
#endif

    /**
     * \class ReplIndividualTask
     * An individual task that is aware that it is 
     * being executed in a control replication context.
     */
    class ReplIndividualTask : public IndividualTask {
    public:
      ReplIndividualTask(Runtime *rt);
      ReplIndividualTask(const ReplIndividualTask &rhs);
      virtual ~ReplIndividualTask(void);
    public:
      ReplIndividualTask& operator=(const ReplIndividualTask &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_prepipeline_stage(void);
      virtual void trigger_ready(void);
    protected:
      ShardingID sharding_functor;
    };

    /**
     * \class ReplIndexTask
     * An individual task that is aware that it is 
     * being executed in a control replication context.
     */
    class ReplIndexTask : public IndexTask {
    public:
      ReplIndexTask(Runtime *rt);
      ReplIndexTask(const ReplIndexTask &rhs);
      virtual ~ReplIndexTask(void);
    public:
      ReplIndexTask& operator=(const ReplIndexTask &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_prepipeline_stage(void);
      virtual void trigger_ready(void);
    protected:
      ShardingID sharding_functor;
    };

    /**
     * \class ReplIndexFillOp
     * An index fill operation that is aware that it is 
     * being executed in a control replication context.
     */
    class ReplIndexFillOp : public IndexFillOp {
    public:
      ReplIndexFillOp(Runtime *rt);
      ReplIndexFillOp(const ReplIndexFillOp &rhs);
      virtual ~ReplIndexFillOp(void);
    public:
      ReplIndexFillOp& operator=(const ReplIndexFillOp &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_prepipeline_stage(void);
      virtual void trigger_ready(void);
    protected:
      ShardingID sharding_functor;
      MapperManager *mapper;
    };

    /**
     * \class ReplCopyOp
     * A fill operation that is aware that it is being
     * executed in a control replication context.
     */
    class ReplCopyOp : public CopyOp {
    public:
      ReplCopyOp(Runtime *rt);
      ReplCopyOp(const ReplCopyOp &rhs);
      virtual ~ReplCopyOp(void);
    public:
      ReplCopyOp& operator=(const ReplCopyOp &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_prepipeline_stage(void);
      virtual void trigger_ready(void);
    protected:
      ShardingID sharding_functor;
    };

    /**
     * \class ReplIndexCopyOp
     * An index fill operation that is aware that it is 
     * being executed in a control replication context.
     */
    class ReplIndexCopyOp : public IndexCopyOp {
    public:
      ReplIndexCopyOp(Runtime *rt);
      ReplIndexCopyOp(const ReplIndexCopyOp &rhs);
      virtual ~ReplIndexCopyOp(void);
    public:
      ReplIndexCopyOp& operator=(const ReplIndexCopyOp &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_prepipeline_stage(void);
      virtual void trigger_ready(void);
    protected:
      ShardingID sharding_functor;
    };

    /**
     * \class ReplDeletionOp
     * A deletion operation that is aware that it is
     * being executed in a control replication context.
     */
    class ReplDeletionOp : public DeletionOp {
    public:
      ReplDeletionOp(Runtime *rt);
      ReplDeletionOp(const ReplDeletionOp &rhs);
      virtual ~ReplDeletionOp(void);
    public:
      ReplDeletionOp& operator=(const ReplDeletionOp &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_ready(void);
    };

    /**
     * \class ReplPendingPartitionOp
     * A pending partition operation that knows that its
     * being executed in a control replication context
     */
    class ReplPendingPartitionOp : public PendingPartitionOp {
    public:
      ReplPendingPartitionOp(Runtime *rt);
      ReplPendingPartitionOp(const ReplPendingPartitionOp &rhs);
      virtual ~ReplPendingPartitionOp(void);
    public:
      ReplPendingPartitionOp& operator=(const ReplPendingPartitionOp &rhs);
    };

    /**
     * \class ReplDependentPartitionOp
     * A dependent partitioning operation that knows that it
     * is being executed in a control replication context
     */
    class ReplDependentPartitionOp : public DependentPartitionOp {
    public:
      ReplDependentPartitionOp(Runtime *rt);
      ReplDependentPartitionOp(const ReplDependentPartitionOp &rhs);
      virtual ~ReplDependentPartitionOp(void);
    public:
      ReplDependentPartitionOp& operator=(const ReplDependentPartitionOp &rhs);
    };

    /**
     * \class ReplMustEpochOp
     * A must epoch operation that is aware that it is 
     * being executed in a control replication context
     */
    class ReplMustEpochOp : public MustEpochOp {
    public:
      ReplMustEpochOp(Runtime *rt);
      ReplMustEpochOp(const ReplMustEpochOp &rhs);
      virtual ~ReplMustEpochOp(void);
    public:
      ReplMustEpochOp& operator=(const ReplMustEpochOp &rhs);
    };

    /**
     * \class ReplTimingOp
     * A timing operation that is aware that it is 
     * being executed in a control replication context
     */
    class ReplTimingOp : public TimingOp {
    public:
      ReplTimingOp(Runtime *rt);
      ReplTimingOp(const ReplTimingOp &rhs);
      virtual ~ReplTimingOp(void);
    public:
      ReplTimingOp& operator=(const ReplTimingOp &rhs);
    public:
      virtual void activate(void);
      virtual void deactivate(void);
    public:
      virtual void trigger_ready(void);
    };

  }; // namespace Internal
}; // namespace Legion

#endif // __LEGION_REPLICATION_H__
