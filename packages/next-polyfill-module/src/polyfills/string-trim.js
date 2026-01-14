/* eslint-disable no-extend-native */

/**
 * String.prototype.trimStart / trimEnd polyfill
 *
 * Available in:
 * Edge: never
 * Firefox: 61
 * Chrome: 66
 * Safari: 12
 *
 * https://caniuse.com/mdn-javascript_builtins_string_trimstart
 * https://caniuse.com/mdn-javascript_builtins_string_trimend
 */
if (!String.prototype.trimStart) {
  String.prototype.trimStart = String.prototype.trimLeft
}
if (!String.prototype.trimEnd) {
  String.prototype.trimEnd = String.prototype.trimRight
}
