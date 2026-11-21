//! Records shared by the write-ahead log and the segment files.

use crate::codec::{corrupt, Dec, Enc, Result};

pub const NONE: u32 = u32::MAX;

/// Note targets: 0 = system piece, 1 = tools piece, 2 + i = message i of the request.
pub const TARGET_SYSTEM: u32 = 0;
pub const TARGET_TOOLS: u32 = 1;
pub const TARGET_MSG0: u32 = 2;

/// Piece kinds, kept in the shared store and deduplicated by hash.
pub const PIECE_SKELETON: u8 = 1;
pub const PIECE_SYSTEM: u8 = 2;
pub const PIECE_TOOLS: u8 = 3;
pub const PIECE_MARKER: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReqNote {
    pub target: u32,
    pub offset: u32,
    /// Piece id of the cut text (a `PIECE_MARKER`).
    pub text: u32,
}

/// What the store keeps for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReqRec {
    pub id: Box<str>,
    pub time: Box<str>,
    pub meta: Box<[u8]>,
    pub session: u32,
    /// Last node of the request's path through the session tree (`NONE` if no messages).
    pub tip: u32,
    pub n_msgs: u32,
    pub skeleton: u32,
    pub system: u32,
    pub tools: u32,
    pub notes: Box<[ReqNote]>,
    /// sha256 of the body as it arrived.
    pub digest: [u8; 32],
    pub layout: Option<Box<[Box<[u8]>]>>,
}

fn opt(v: u32) -> u64 {
    if v == NONE { 0 } else { v as u64 + 1 }
}

fn unopt(v: u64) -> Result<u32> {
    if v == 0 {
        Ok(NONE)
    } else {
        u32::try_from(v - 1).map_err(|_| crate::codec::Error::Corrupt("id over u32".into()))
    }
}

impl ReqRec {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(self.id.as_bytes());
        e.bytes(self.time.as_bytes());
        e.bytes(&self.meta);
        e.var(self.session as u64);
        e.var(opt(self.tip));
        e.var(self.n_msgs as u64);
        e.var(self.skeleton as u64);
        e.var(opt(self.system));
        e.var(opt(self.tools));
        e.var(self.notes.len() as u64);
        for n in self.notes.iter() {
            e.var(n.target as u64);
            e.var(n.offset as u64);
            e.var(n.text as u64);
        }
        e.raw(&self.digest);
        match &self.layout {
            None => e.u8(0),
            Some(g) => {
                e.u8(1);
                e.var(g.len() as u64);
                for x in g.iter() {
                    e.bytes(x);
                }
            }
        }
    }

    pub fn decode(d: &mut Dec) -> Result<ReqRec> {
        let id = d.string()?.into_boxed_str();
        let time = d.string()?.into_boxed_str();
        let meta = d.bytes()?.to_vec().into_boxed_slice();
        let session = d.var32()?;
        let tip = unopt(d.var()?)?;
        let n_msgs = d.var32()?;
        let skeleton = d.var32()?;
        let system = unopt(d.var()?)?;
        let tools = unopt(d.var()?)?;
        let nn = d.var()? as usize;
        let mut notes = Vec::with_capacity(nn);
        for _ in 0..nn {
            notes.push(ReqNote { target: d.var32()?, offset: d.var32()?, text: d.var32()? });
        }
        let digest: [u8; 32] = d.raw(32)?.try_into().unwrap();
        let layout = match d.u8()? {
            0 => None,
            1 => {
                let n = d.var()? as usize;
                let mut g = Vec::with_capacity(n);
                for _ in 0..n {
                    g.push(d.bytes()?.to_vec().into_boxed_slice());
                }
                Some(g.into_boxed_slice())
            }
            _ => return corrupt("bad layout flag"),
        };
        Ok(ReqRec { id, time, meta, session, tip, n_msgs, skeleton, system, tools, notes: notes.into_boxed_slice(), digest, layout })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNode {
    pub id: u32,
    pub parent: u32,
    pub hash: u128,
    pub bytes: Vec<u8>,
    /// (offset in `bytes`, opaque id)
    pub holes: Vec<(u32, u64)>,
}

/// An opaque value new to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOpaque {
    pub id: u64,
    pub enc: u8,
    pub kind: u8,
    pub hash: u128,
    /// The value; empty when it is not kept.
    pub payload: Vec<u8>,
    /// For a value not kept: the length its payload had.
    pub dropped: Option<u32>,
}

/// One write-ahead log record: everything one request adds to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    pub seq: u64,
    pub new_session: Option<(u32, String, String)>,
    pub pieces: Vec<(u32, u8, u128, Vec<u8>)>,
    pub opaques: Vec<NewOpaque>,
    pub session: u32,
    pub nodes: Vec<NewNode>,
    pub req: ReqRec,
}

const TAG_PUT: u8 = 2;

impl Mutation {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u8(TAG_PUT);
        e.var(self.seq);
        match &self.new_session {
            None => e.u8(0),
            Some((idx, s, a)) => {
                e.u8(1);
                e.var(*idx as u64);
                e.bytes(s.as_bytes());
                e.bytes(a.as_bytes());
            }
        }
        e.var(self.pieces.len() as u64);
        for (id, kind, h, b) in &self.pieces {
            e.var(*id as u64);
            e.u8(*kind);
            e.u128(*h);
            e.bytes(b);
        }
        e.var(self.opaques.len() as u64);
        for o in &self.opaques {
            e.var(o.id);
            e.u8(o.enc);
            e.u8(o.kind | if o.dropped.is_some() { 0x80 } else { 0 });
            e.u128(o.hash);
            match o.dropped {
                Some(n) => e.var(n as u64),
                None => e.bytes(&o.payload),
            }
        }
        e.var(self.session as u64);
        e.var(self.nodes.len() as u64);
        for n in &self.nodes {
            e.var(n.id as u64);
            e.var(opt(n.parent));
            e.u128(n.hash);
            e.bytes(&n.bytes);
            e.var(n.holes.len() as u64);
            for &(o, id) in &n.holes {
                e.var(o as u64);
                e.var(id);
            }
        }
        self.req.encode(&mut e);
        e.buf
    }

    pub fn decode(b: &[u8]) -> Result<Mutation> {
        let mut d = Dec::new(b);
        if d.u8()? != TAG_PUT {
            return corrupt("unknown log record");
        }
        let seq = d.var()?;
        let new_session = match d.u8()? {
            0 => None,
            1 => Some((d.var32()?, d.string()?, d.string()?)),
            _ => return corrupt("bad session flag"),
        };
        let np = d.var()? as usize;
        let mut pieces = Vec::with_capacity(np);
        for _ in 0..np {
            pieces.push((d.var32()?, d.u8()?, d.u128()?, d.bytes()?.to_vec()));
        }
        let no = d.var()? as usize;
        let mut opaques = Vec::with_capacity(no);
        for _ in 0..no {
            let (id, enc, k, hash) = (d.var()?, d.u8()?, d.u8()?, d.u128()?);
            let (payload, dropped) = if k & 0x80 != 0 { (Vec::new(), Some(d.var32()?)) } else { (d.bytes()?.to_vec(), None) };
            opaques.push(NewOpaque { id, enc, kind: k & 0x7f, hash, payload, dropped });
        }
        let session = d.var32()?;
        let nn = d.var()? as usize;
        let mut nodes = Vec::with_capacity(nn);
        for _ in 0..nn {
            let id = d.var32()?;
            let parent = unopt(d.var()?)?;
            let hash = d.u128()?;
            let bytes = d.bytes()?.to_vec();
            let nh = d.var()? as usize;
            let mut holes = Vec::with_capacity(nh);
            for _ in 0..nh {
                holes.push((d.var32()?, d.var()?));
            }
            nodes.push(NewNode { id, parent, hash, bytes, holes });
        }
        let req = ReqRec::decode(&mut d)?;
        if !d.done() {
            return corrupt("trailing bytes in log record");
        }
        Ok(Mutation { seq, new_session, pieces, opaques, session, nodes, req })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_roundtrip() {
        let m = Mutation {
            seq: 42,
            new_session: Some((3, "s".into(), "a".into())),
            pieces: vec![(1, PIECE_SKELETON, 99, b"sk".to_vec())],
            opaques: vec![
                NewOpaque { id: 7, enc: 0, kind: 2, hash: 5, payload: vec![1, 2, 3], dropped: None },
                NewOpaque { id: 8, enc: 1, kind: 1, hash: 6, payload: Vec::new(), dropped: Some(300) },
            ],
            session: 3,
            nodes: vec![NewNode { id: 0, parent: NONE, hash: 1, bytes: b"{}".to_vec(), holes: vec![(1, 7)] }],
            req: ReqRec {
                id: "r1".into(),
                time: "t".into(),
                meta: Box::new([]),
                session: 3,
                tip: 0,
                n_msgs: 1,
                skeleton: 1,
                system: NONE,
                tools: NONE,
                notes: vec![ReqNote { target: 2, offset: 1, text: 4 }].into_boxed_slice(),
                digest: [7; 32],
                layout: Some(vec![b" ".to_vec().into_boxed_slice()].into_boxed_slice()),
            },
        };
        assert_eq!(Mutation::decode(&m.encode()).unwrap(), m);
    }
}
