package wire

// FoundationDB 8.0.0 removes TenantInfo from public RPCs. These layouts follow
// fdbclient/include/fdbclient/{StorageServerInterface,CommitProxyInterface}.h
// in the 8.0.0 source, not the 7.x schema with a missing tenant value: fields
// following a removed member move to different ObjectSerializer slots.

func encode80GetValue(b *Builder, r *GetValue, reply UID) Off {
	key := b.Bytes(r.Key)
	var tags, options Off
	if len(r.Ctx.Tags) > 0 {
		tags = tagSet(b, r.Ctx.Tags)
	}
	if r.Ctx.HasOptions {
		options = readOptions(b, r.Ctx.Options)
	}
	rp := replyPromise(b, reply)
	b.StartTable(9)
	b.AddOffset(0, key)
	b.AddI64(1, r.Ctx.Version)
	if tags != 0 {
		b.AddUnion(2, 1, tags)
	}
	b.AddOffset(4, rp)
	if options != 0 {
		b.AddUnion(6, 1, options)
	}
	return b.EndTable()
}

func encode80GetKey(b *Builder, r *GetKey, reply UID) Off {
	sel := keySelector(b, &r.Sel)
	var tags, options Off
	if len(r.Ctx.Tags) > 0 {
		tags = tagSet(b, r.Ctx.Tags)
	}
	if r.Ctx.HasOptions {
		options = readOptions(b, r.Ctx.Options)
	}
	rp := replyPromise(b, reply)
	b.StartTable(9)
	b.AddOffset(0, sel)
	b.AddI64(1, r.Ctx.Version)
	if tags != 0 {
		b.AddUnion(2, 1, tags)
	}
	b.AddOffset(4, rp)
	if options != 0 {
		b.AddUnion(6, 1, options)
	}
	return b.EndTable()
}

func encode80GetKeyValues(b *Builder, r *GetKeyValues, reply UID) Off {
	begin := keySelector(b, &r.Begin)
	end := keySelector(b, &r.End)
	var tags, options Off
	if len(r.Ctx.Tags) > 0 {
		tags = tagSet(b, r.Ctx.Tags)
	}
	if r.Ctx.HasOptions {
		options = readOptions(b, r.Ctx.Options)
	}
	rp := replyPromise(b, reply)
	// options(2), ssLatestCommitVersions, taskID(2); taskID is absent.
	b.StartTable(14)
	b.AddOffset(0, begin)
	b.AddOffset(1, end)
	b.AddI64(2, r.Ctx.Version)
	b.AddI32(3, r.Limit)
	b.AddI32(4, r.LimitBytes)
	if tags != 0 {
		b.AddUnion(5, 1, tags)
	}
	b.AddOffset(7, rp)
	if options != 0 {
		b.AddUnion(9, 1, options)
	}
	return b.EndTable()
}

func encode80GetMappedKeyValues(b *Builder, r *GetMappedKeyValues, reply UID) Off {
	begin := keySelector(b, &r.Begin)
	end := keySelector(b, &r.End)
	mapper := b.Bytes(r.Mapper)
	var tags, options Off
	if len(r.Ctx.Tags) > 0 {
		tags = tagSet(b, r.Ctx.Tags)
	}
	if r.Ctx.HasOptions {
		options = readOptions(b, r.Ctx.Options)
	}
	rp := replyPromise(b, reply)
	b.StartTable(15)
	b.AddOffset(0, begin)
	b.AddOffset(1, end)
	b.AddOffset(2, mapper)
	b.AddI64(3, r.Ctx.Version)
	b.AddI32(4, r.Limit)
	b.AddI32(5, r.LimitBytes)
	if tags != 0 {
		b.AddUnion(6, 1, tags)
	}
	b.AddOffset(8, rp)
	if options != 0 {
		b.AddUnion(10, 1, options)
	}
	return b.EndTable()
}

func encode80WatchValue(b *Builder, r *WatchValue, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	key := b.Bytes(r.Key)
	var value, tags Off
	if r.Value != nil {
		value = b.Bytes(r.Value)
	}
	if len(r.Tags) > 0 {
		tags = tagSet(b, r.Tags)
	}
	rp := replyPromise(b, reply)
	b.StartTable(10)
	b.AddOffset(0, key)
	if r.Value != nil {
		b.AddUnion(1, 1, value)
	}
	b.AddI64(3, r.Version)
	if tags != 0 {
		b.AddUnion(4, 1, tags)
	}
	if debug != 0 {
		b.AddUnion(6, 1, debug)
	}
	b.AddOffset(8, rp)
	return b.EndTable()
}

func encode80GetKeyServerLocations(b *Builder, r GetKeyServerLocations, reply UID) Off {
	begin := b.Bytes(r.Begin)
	var end Off
	if r.End != nil {
		end = b.Bytes(r.End)
	}
	rp := replyPromise(b, reply)
	b.StartTable(8)
	b.AddOffset(0, begin)
	if r.End != nil {
		b.AddUnion(1, 1, end)
	}
	b.AddI32(3, r.Limit)
	b.AddBool(4, r.Reverse)
	b.AddOffset(5, rp)
	b.AddI64(7, LatestVersion)
	return b.EndTable()
}

func encode80ExpireIdempotencyID(b *Builder, r ExpireIdempotencyID, _ UID) Off {
	b.StartTable(2)
	b.AddI64(0, r.Version)
	b.AddU8(1, r.BatchIndexHighByte)
	return b.EndTable()
}

func encode80WaitMetrics(b *Builder, r *WaitMetrics, reply UID) Off {
	keys := keyRange(b, r.Range.Begin, r.Range.End)
	min := storageMetrics(b, &r.Min)
	max := storageMetrics(b, &r.Max)
	rp := replyPromise(b, reply)
	b.StartTable(5)
	b.AddOffset(0, keys)
	b.AddOffset(1, min)
	b.AddOffset(2, max)
	b.AddOffset(3, rp)
	// legacyVersion replaces the old tenantInfo/minVersion pair. It is unused
	// by storage servers; send zero rather than a TenantInfo table offset.
	b.AddI64(4, 0)
	return b.EndTable()
}

func encode80SplitRange(b *Builder, r *SplitRange, reply UID) Off {
	keys := keyRange(b, r.Range.Begin, r.Range.End)
	rp := replyPromise(b, reply)
	b.StartTable(4)
	b.AddOffset(0, keys)
	b.AddI64(1, r.ChunkSize)
	b.AddOffset(2, rp)
	// 8.0 adds a signed limit where 7.x carried TenantInfo. FlatBuffers uses
	// scalar zero for an absent field, not the C++ member initializer (-1).
	b.AddI32(3, -1)
	return b.EndTable()
}

func encode80Commit(b *Builder, r *Commit, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	m := b.Mark()
	for i := range r.ReadConflicts {
		b.Push(keyRange(b, r.ReadConflicts[i].Begin, r.ReadConflicts[i].End))
	}
	rcr := b.Offsets(m)
	for i := range r.WriteConflicts {
		b.Push(keyRange(b, r.WriteConflicts[i].Begin, r.WriteConflicts[i].End))
	}
	wcr := b.Offsets(m)
	for i := range r.Mutations {
		mu := &r.Mutations[i]
		p1 := b.Bytes(mu.Param1)
		p2 := b.Bytes(mu.Param2)
		b.StartTable(3)
		b.AddU8(0, mu.Kind)
		b.AddOffset(1, p1)
		b.AddOffset(2, p2)
		b.Push(b.EndTable())
	}
	muts := b.Offsets(m)
	// CommitTransactionRef retains spanContext(2) and tenantIds(2) in 8.0.
	b.StartTable(10)
	b.AddOffset(0, rcr)
	b.AddOffset(1, wcr)
	b.AddOffset(2, muts)
	b.AddI64(3, r.ReadSnapshot)
	b.AddBool(4, r.ReportConflictingKeys)
	b.AddBool(5, r.LockAware)
	tx := b.EndTable()

	var tags, idmp, cost Off
	if c := r.CostEstimation; c != nil {
		for _, clear := range c.ClearCosts {
			b.StartTable(2)
			b.AddI32(0, clear.MutationIndex)
			b.AddI64(1, int64(clear.Cost))
			b.Push(b.EndTable())
		}
		clears := b.Offsets(m)
		b.StartTable(4)
		b.AddI32(0, c.OpsCount)
		b.AddI64(1, int64(c.WriteCosts))
		b.AddOffset(2, clears)
		b.AddU32(3, c.ExpensiveCostEstCount)
		cost = b.EndTable()
	}
	if len(r.Tags) > 0 {
		tags = tagSet(b, r.Tags)
	}
	if r.IdempotencyID != nil {
		idmp = b.Bytes(r.IdempotencyID)
	}
	rp := replyPromise(b, reply)
	b.StartTable(11)
	b.AddOffset(0, tx)
	b.AddOffset(1, rp)
	b.AddU32(2, r.Flags)
	if debug != 0 {
		b.AddUnion(3, 1, debug)
	}
	if cost != 0 {
		b.AddUnion(5, 1, cost)
	}
	if tags != 0 {
		b.AddUnion(7, 1, tags)
	}
	if r.IdempotencyID != nil {
		b.AddOffset(10, idmp)
	}
	return b.EndTable()
}
