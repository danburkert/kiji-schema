/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.kiji.schema;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * A view of a row in a Kiji table.
 *
 * <p>
 *   The view allows access to the columns specified in a {@code KijiDataRequest}. The primary
 *   method of accessing the {@link KijiCell}s in the row is through the {@link #iterator()} method.
 *   The {@link #getColumnResult} method can be used to narrow the view of the row to a specific
 *   column or column family.
 * </p>
 *
 * <h2>Type Safety</h2>
 *
 * <p>
 *   The {@code KijiResult} API is not compile-time type safe if {@link #getColumnResult} is used
 *   inappropriately. {@code getColumnResult} allows the caller to specify the value-type of
 *   {@code KijiCell}s in the requested column. If the wrong type is supplied, a runtime
 *   {@link java.lang.ClassCastException} will be thrown.
 * </p>
 *
 * <p>
 *   In particular, users should be cautious of specifying a type-bound (other than {@link Object})
 *   on group-type families. It is only appropriate to specify a more specific type bound over a
 *   group-type family when the family contains columns of a single type, or the
 *   {@code KijiDataRequest} only contains columns from the family of a single type.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 *   {@code KijiResult} implementations may not be relied upon to be thread safe, and thus should
 *   not be shared between threads.
 * </p>
 *
 * @param <T> the type of {@code KijiCell} values in the view. The result of scans and gets have a
 *    type of {@code Object} to indicate that a specific bound over arbitrary columns can not be
 *    inferred. Users may request a more specific type when calling {@link #getColumnResult},
 *    but be aware of the type-safety warnings above.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiResult<T> extends Iterable<KijiCell<T>> {

  /**
   * Get the EntityId of this KijiResult.
   *
   * @return the EntityId of this KijiResult.
   */
  EntityId getEntityId();

  /**
   * Get the data request which defines this KijiResult.
   *
   * @return the data request which defines this KijiResult.
   */
  KijiDataRequest getDataRequest();

  /**
   * {@inheritDoc}
   *
   * <p>
   *   Multiple calls to {@code iterator()} will return independent iterators. If the
   *   {@code DataRequest} which defines this {@code KijiResult} contains paged columns, those pages
   *   will be requested from the underlying store for each call to this method. Two active
   *   iterators may hold two pages of data in memory simultaneously.
   * </p>
   *
   * <h2>Ordering</h2>
   *
   * <p>
   *   {@code KijiResult} provides four guarantees about the ordering of the {@code KijiCell}s
   *   in the returned iterator:
   * </p>
   *
   * <ol>
   *   <li> Non-paged columns appear before paged columns.
   *   <li> Within a column family, qualified columns will appear in ascending qualifier order.
   *   <li> Within a qualified-column, {@code KijiCell}s will appear in descending version order.
   *   <li> Relative ordering of columns will be consistent in repeated calls to
   *        {@link #iterator()} for the same {@code KijiResult}.
   * </ol>
   *
   * <p>
   *   Where two of these guarantees apply, the guarantee higher in the list takes precedence. For
   *   instance, if a {@code KijiDataRequest} contains requests for a paged column "fam:qual1",
   *   and a non-paged column "fam:qual2", then "fam:qual2" will be returned first in the iterator,
   *   despite {@code "qual1"} sorting before {@code "qual2"}.
   * </p>
   *
   * <p>
   *   No guarantee is made about the ordering among families.
   * </p>
   *
   * @return an iterator over all the cells in this KijiResult.
   * @throws KijiIOException on unrecoverable I/O exception.
   */
  @Override
  Iterator<KijiCell<T>> iterator();

  /**
   * Get a view of this {@code KijiResult} restricted to the provided column.
   *
   * <p>
   *   The column may be a column family, or a fully-qualified column. If this {@code KijiResult}
   *   does not include the provided column, then the resulting {@code KijiResult} will be empty,
   *   and its {@code KijiDataRequest} will not contain any columns. If the provided column is
   *   non-paged, then the cells will be shared with the returned {@code KijiResult}.
   * </p>
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the provided column.
   *   The caller should be careful to only specify an appropriate type. If the type is too specific
   *   (or wrong), runtime {@link java.lang.ClassCastException} will be thrown when the returned
   *   {@code KijiResult} is used. See the Type Safety section of {@code KijiResult}'s documentation
   *   for more details.
   * </p>
   *
   * @param column which the view of this {@code KijiResult} will be restricted.
   * @param <T> type of the value in the given column.
   * @return a {@code KijiResult} which contains only the provided column.
   */
  <T> KijiResult<T> getColumnResult(KijiColumnName column);

  /**
   * Provides helper methods for working with {@link KijiResult}s.
   */
  public static class Helpers {

    /**
     * Returns a {@link com.google.common.collect.Multiset} of {@code KijiColumnName} to version
     * count for the columns in the provided {@code KijiResult}.
     *
     * <p>
     *   The returned {@code Multiset}'s iteration order is the order of columns returned by this
     *   {@code KijiResult}, and the count is the number of versions of the column. Note that this
     *   will require fetching paged columns.
     * </p>
     *
     * <p>
     *   If the provided {@code KijiResult} will only be used retrieving the columns of the row, the
     *   {@link org.kiji.schema.filter.StripValueRowFilter} or
     *   {@link org.kiji.schema.filter.StripValueColumnFilter} can be used to avoid retrieving the
     *   values of each {@code KijiCell}.
     * </p>
     *
     * <p>
     *   If the provided {@code KijiResult} will only be used retrieving the columns of the row, and
     *   the version count is not important, set the max number of versions to retrieve in the
     *   {@link KijiDataRequest} for each column to {@code 1} to avoid retrieving unnecessary
     *   {@code KeyValues}s.
     * </p>
     *
     * @param result {@code KijiResult} for which to retrieve columns and version counts.
     * @return a {@code Multiset} of {@code KijiColumnName} to version count.
     */
    LinkedHashMultiset<KijiColumnName> getColumns(KijiResult<?> result) {
      final LinkedHashMultiset<KijiColumnName> set = LinkedHashMultiset.create();
      for (KijiCell<?> cell : result) {
        set.add(cell.getColumn());
      }
      return set;
    }

    /**
     * Return the first {@code KijiCell} in the first column of this {@code KijiResult}.
     *
     * <p>
     *   Note that if all columns in the {@code KijiResult} are paged, then retrieving the first
     *   cell will require fetching a full page.
     * </p>
     *
     * @param <T> type of {@code KijiCell}s in the provided {@code KijiResult}.
     * @return the first {@code KijiCell} in the result, or {@code null} if the result is empty.
     */
    <T> KijiCell<T> getFirst(KijiResult<T> result) {
      return Iterables.getFirst(result, null);
    }

    /**
     * Return the value of the first {@code KijiCell} in the first column of this
     * {@code KijiResult}.
     *
     * <p>
     *   Note that if all columns in the {@code KijiResult} are paged, then retrieving the first
     *   cell will require fetching a full page.
     * </p>
     *
     * <p>
     *   It is not possible to distinguish between an empty result and a {@code KijiCell} which
     *   contains a null value using this method. If the difference is material, use
     *   {@link #getFirst} and explicitly check whether the resulting {@code KijiCell} is
     *   {@code null}.
     * </p>
     *
     * @param <T> type of {@code KijiCell}s in the provided {@code KijiResult}.
     * @return the value of the first {@code KijiCell} in the result, or {@code null} if the result
     *    is empty, or the value is {@code null}.
     */
    <T> T getFirstValue(KijiResult<T> result) {
      final KijiCell<T> first = getFirst(result);
      if (first == null) {
        return null;
      } else {
        return first.getData();
      }
    }

    /**
     * Return an {@link Iterable} of the {@code KijiCell} values in this {@code KijiResult}. Note
     * that the returned {@code Iterable} will require fetching paged columns when iterated exactly
     * as {@code KijiResult}s do.
     *
     * @param result the {@code KijiResult} from which to retrieve values.
     * @param <T> type of values in the {@code KijiResult}.
     * @return the values in the provided {@code KijiResult}.
     */
    <T> Iterable<T> getValues(KijiResult<T> result) {
      return Iterables.transform(
          result,
          new Function<KijiCell<T>, T>() {
            @Override
            public T apply(final KijiCell<T> cell) {
              return cell.getData();
            }
          });
    }

    /**
     * Private constructor for utility class.
     */
    private Helpers() {
    }
  }
}
