package com.amazon.milan


object SemanticVersion {
  /**
   * A [[SemanticVersion]] object with all version numbers as zero.
   */
  val ZERO: SemanticVersion = SemanticVersion(0, 0, 0)

  def parse(versionString: String): SemanticVersion = {
    val majorEnd = versionString.indexOf(".")
    val minorEnd = versionString.indexOf(".", majorEnd + 1)
    val hyphenIndex = versionString.indexOf("-")
    val plusIndex = versionString.indexOf("+")

    // The patch number ends at the first instance of a +, -, or end of string.
    val patchEnd = (hyphenIndex, plusIndex) match {
      case (-1, -1) => versionString.length
      case (i, -1) => i
      case (-1, i) => i
      case (a, b) => Math.min(a, b)
    }

    val major = versionString.substring(0, majorEnd).toInt
    val minor = versionString.substring(majorEnd + 1, minorEnd).toInt
    val patch = versionString.substring(minorEnd + 1, patchEnd).toInt

    val preReleaseStart = hyphenIndex match {
      case -1 =>
        // No hyphen means definitely no pre-release version.
        -1
      case i if i < plusIndex || plusIndex < 0 =>
        // A hyphen before a plus means there is a pre-release version.
        i + 1
      case _ =>
        // A hyphen after a plus no pre-release version because the hyphen is part of the build metadata.
        -1
    }

    val preReleaseEnd = plusIndex match {
      case -1 => versionString.length
      case i => i
    }

    val preRelease = if (preReleaseStart > 0) versionString.substring(preReleaseStart, preReleaseEnd) else null
    val buildMetadata = if (plusIndex > 0) versionString.substring(plusIndex + 1) else null

    new SemanticVersion(major, minor, patch, preRelease, buildMetadata)
  }
}


/**
 * Represents a semantic version. See http://semver.org for the definition.
 *
 * @param major         The major version number.
 * @param minor         The minor version number.
 * @param patch         The patch number.
 * @param preRelease    Any pre-release version information.
 * @param buildMetadata Build metadata.
 */
case class SemanticVersion(var major: Int,
                           var minor: Int,
                           var patch: Int,
                           var preRelease: String = null,
                           var buildMetadata: String = null)
  extends Comparable[SemanticVersion]
    with Serializable {

  def this() {
    this(0, 0, 0)
  }

  /**
   * Gets a SemanticVersion object that is equivalent to this one with any build metadata information removed.
   *
   * @return A SemanticVersion instance with no build metadata.
   */
  def withoutBuildMetadata(): SemanticVersion = SemanticVersion(this.major, this.minor, this.patch, this.preRelease)

  /**
   * Gets whether this version contains pre-release information.
   *
   * @return True if pre-release information is present (not null or empty), otherwise false.
   */
  def hasPreRelease: Boolean = this.preRelease != null && this.preRelease.nonEmpty

  /**
   * Compare this semantic version to another. Returns 1 if this version represents a later version than the other,
   * 0 if they are equivalent, and -1 if this version is earlier than the other. Build metadata is ignored in this
   * comparison.
   *
   * @param other Another semantic version.
   * @return 1 if this version represents a later version than the other, 0 if they are equivalent,
   *         and -1 if this version is earlier than the other.
   */
  override def compareTo(other: SemanticVersion): Int = {
    // If the major versions are different then that has precedence.
    this.major.compareTo(other.major) match {
      case 1 => return 1
      case -1 => return -1
      case _ => ()
    }

    // If the minor versions are different then that has precedence.
    this.minor.compareTo(other.minor) match {
      case 1 => return 1
      case -1 => return -1
      case _ => ()
    }

    this.patch.compareTo(other.patch) match {
      case 1 => return 1
      case -1 => return -1
      case _ => ()
    }

    (this.hasPreRelease, other.hasPreRelease) match {
      case (false, false) =>
        0

      case (true, false) =>
        // This semver has pre-release info and the other does not, which means the other version is later.
        -1

      case (false, true) =>
        // This semver does not have pre-release info and the other does, which means this version is later.
        1

      case (true, true) =>
        // They both have pre-release info so compare them.
        this.comparePreReleaseVersions(this.preRelease, other.preRelease)
    }
  }

  /**
   * Checks for equality with another object. This does not apply proper semantic version number equality checks,
   * because it also checks for equality between any build metadata. Use the compareTo() method to check for version
   * precedence.
   *
   * @param obj An object.
   * @return True if the objects are equal, otherwise false.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SemanticVersion =>
        this.major == other.major &&
          this.minor == other.minor &&
          this.patch == other.patch &&
          this.preRelease == other.preRelease &&
          this.buildMetadata == other.buildMetadata
    }
  }

  /**
   * Test if this SemanticVersion instance represents an earlier version than another.
   *
   * @param other Another SemanticVersion instance.
   * @return True if this instance represents an earlier version, otherwise false.
   */
  def <(other: SemanticVersion): Boolean = {
    this.compareTo(other) < 0
  }

  /**
   * Test if this SemanticVersion instance represents a later version than another.
   *
   * @param other Another SemanticVersion instance.
   * @return True if this instance represents a later version, otherwise false.
   */
  def >(other: SemanticVersion): Boolean = {
    this.compareTo(other) > 0
  }

  override def toString: String = {
    val builder = new StringBuilder(s"${this.major}.${this.minor}.${this.patch}")

    if (this.preRelease != null && this.preRelease.nonEmpty) {
      builder.append(s"-${
        this.preRelease
      }")
    }

    if (this.buildMetadata != null && this.buildMetadata.nonEmpty) {
      builder.append(s"+${
        this.buildMetadata
      }")
    }

    builder.toString()
  }

  /**
   * Compares to pre-release version strings. Returns 1 if the first represents a later version,
   * 0 if they are equivalent, and -1 if the first represents an earlier version.
   *
   * @param first  The first pre-release version string.
   * @param second The second pre-release version string.
   */
  private def comparePreReleaseVersions(first: String, second: String): Int = {
    val firstParts = first.split('.')
    val secondParts = second.split('.')
    val partsToCompare = Math.min(firstParts.length, secondParts.length)

    // Compare parts of the pre-release versions until we find one that isn't equivalent.
    // If they are all equivalent then whichever has more parts is the later version.

    firstParts.take(partsToCompare)
      .zip(secondParts.take(partsToCompare))
      .map {
        case (firstPart, secondPart) => this.comparePreReleaseParts(firstPart, secondPart)
      }
      .find(c => c != 0)
      .getOrElse(firstParts.length.compareTo(secondParts.length))
  }

  /**
   * Compares to pre-release version string parts. Returns 1 if the first represents a later version,
   * 0 if they are equivalent, and -1 if the first represents an earlier version.
   *
   * @param first  The first pre-release version string part.
   * @param second The second pre-release version string part.
   */
  private def comparePreReleaseParts(first: String, second: String): Int = {
    val firstIsNumber = first.forall(Character.isDigit)
    val secondIsNumber = second.forall(Character.isDigit)

    if (firstIsNumber && secondIsNumber) {
      first.toInt.compareTo(second.toInt)
    }
    else if (firstIsNumber) {
      -1
    }
    else if (secondIsNumber) {
      1
    }
    else {
      first.compareTo(second)
    }
  }
}
