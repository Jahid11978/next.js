import React from 'react'
import { getProperError } from '../../../../lib/is-error'

const ownerStacks = new WeakMap<Error, string | null>()

export function getOwnerStack(error: Error): string | null | undefined {
  return ownerStacks.get(error)
}
export function setOwnerStack(error: Error, stack: string | null) {
  ownerStacks.set(error, stack)
}

export function setOwnerStackIfAvailable(error: Error): void {
  // React 18 and prod does not have `captureOwnerStack`
  if ('captureOwnerStack' in React) {
    setOwnerStack(error, React.captureOwnerStack())
  }
}

export function decorateDevError(thrownValue: unknown) {
  const error = getProperError(thrownValue)
  setOwnerStackIfAvailable(error)
  return error
}
