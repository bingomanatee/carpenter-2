import { dataMap, JoinDef, JoinFieldDef } from './types'
import { c } from '@wonderlandlabs/collect'

export function addToIndex(map: dataMap, identity: unknown, otherIdentities: unknown, isList?: boolean) {
  if (isList && Array.isArray(otherIdentities)) {
    otherIdentities.forEach((toItem) => addToIndex(map, identity, toItem));
    return;
  }

  if (!map.has(identity)) {
    map.set(identity, [otherIdentities]);
  } else {
    const IDs = map.get(identity);
    if (!IDs.includes(otherIdentities)) {
      IDs.push(otherIdentities);
    }
  }
}

export function identitiesForMidKeys(midKeys: unknown[], reverseIndex: dataMap): unknown[] {
  const matchingIdentities = new Set();
  midKeys.forEach((midKey) => {
    if (reverseIndex.has(midKey)) {
      const endIdentity = reverseIndex.get(midKey);
      endIdentity.forEach((identity: unknown) => matchingIdentities.add(identity));
    }
  });

  return Array.from(matchingIdentities.values());
}

export function extractFieldDef(def: JoinFieldDef, record: unknown) {
  const coll = c(record);
  if (coll.family === 'container') {
    try {
      const fromIdentity = coll.get(def.field);
      if (fromIdentity !== undefined) {
        if (Array.isArray(fromIdentity)) {
          return fromIdentity;
        } else {
          return [fromIdentity];
        }
      }
    } catch (_err) {
      // cannot retrieve
    }
  }
  return [];
}

export function parseConfig(config: string | JoinDef) {
  if (typeof config === 'string') {
    return {
      table: config,
    }
  } else {
    return config;
  }
}

export const emptyMap = new Map()
