///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2008, Robert D. Eden All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////


package gnu.trove;


import java.util.*;

import gnu.trove.decorator.*;



/**
 * This is a static utility class that provides functions for simplifying creation of
 * decorators.
 *
 * @author  Robert D. Eden
 * @since   Trove 2.1
 */
public class Decorators {
    // Hide the constructor
    private Decorators() {}

    
    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Double> wrap( TDoubleDoubleHashMap map ) {
        return new TDoubleDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Float> wrap( TDoubleFloatHashMap map ) {
        return new TDoubleFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Integer> wrap( TDoubleIntHashMap map ) {
        return new TDoubleIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Long> wrap( TDoubleLongHashMap map ) {
        return new TDoubleLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Byte> wrap( TDoubleByteHashMap map ) {
        return new TDoubleByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Double,Short> wrap( TDoubleShortHashMap map ) {
        return new TDoubleShortHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Double> wrap( TFloatDoubleHashMap map ) {
        return new TFloatDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Float> wrap( TFloatFloatHashMap map ) {
        return new TFloatFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Integer> wrap( TFloatIntHashMap map ) {
        return new TFloatIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Long> wrap( TFloatLongHashMap map ) {
        return new TFloatLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Byte> wrap( TFloatByteHashMap map ) {
        return new TFloatByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Float,Short> wrap( TFloatShortHashMap map ) {
        return new TFloatShortHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Double> wrap( TIntDoubleHashMap map ) {
        return new TIntDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Float> wrap( TIntFloatHashMap map ) {
        return new TIntFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Integer> wrap( TIntIntHashMap map ) {
        return new TIntIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Long> wrap( TIntLongHashMap map ) {
        return new TIntLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Byte> wrap( TIntByteHashMap map ) {
        return new TIntByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Integer,Short> wrap( TIntShortHashMap map ) {
        return new TIntShortHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Double> wrap( TLongDoubleHashMap map ) {
        return new TLongDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Float> wrap( TLongFloatHashMap map ) {
        return new TLongFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Integer> wrap( TLongIntHashMap map ) {
        return new TLongIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Long> wrap( TLongLongHashMap map ) {
        return new TLongLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Byte> wrap( TLongByteHashMap map ) {
        return new TLongByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Long,Short> wrap( TLongShortHashMap map ) {
        return new TLongShortHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Double> wrap( TByteDoubleHashMap map ) {
        return new TByteDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Float> wrap( TByteFloatHashMap map ) {
        return new TByteFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Integer> wrap( TByteIntHashMap map ) {
        return new TByteIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Long> wrap( TByteLongHashMap map ) {
        return new TByteLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Byte> wrap( TByteByteHashMap map ) {
        return new TByteByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Byte,Short> wrap( TByteShortHashMap map ) {
        return new TByteShortHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Double> wrap( TShortDoubleHashMap map ) {
        return new TShortDoubleHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Float> wrap( TShortFloatHashMap map ) {
        return new TShortFloatHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Integer> wrap( TShortIntHashMap map ) {
        return new TShortIntHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Long> wrap( TShortLongHashMap map ) {
        return new TShortLongHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Byte> wrap( TShortByteHashMap map ) {
        return new TShortByteHashMapDecorator( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static Map<Short,Short> wrap( TShortShortHashMap map ) {
        return new TShortShortHashMapDecorator( map );
    }


    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Double> wrap( TObjectDoubleHashMap<T> map ) {
        return new TObjectDoubleHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Float> wrap( TObjectFloatHashMap<T> map ) {
        return new TObjectFloatHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Integer> wrap( TObjectIntHashMap<T> map ) {
        return new TObjectIntHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Long> wrap( TObjectLongHashMap<T> map ) {
        return new TObjectLongHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Byte> wrap( TObjectByteHashMap<T> map ) {
        return new TObjectByteHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<T,Short> wrap( TObjectShortHashMap<T> map ) {
        return new TObjectShortHashMapDecorator<T>( map );
    }


    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Double,T> wrap( TDoubleObjectHashMap<T> map ) {
        return new TDoubleObjectHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Float,T> wrap( TFloatObjectHashMap<T> map ) {
        return new TFloatObjectHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Integer,T> wrap( TIntObjectHashMap<T> map ) {
        return new TIntObjectHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Long,T> wrap( TLongObjectHashMap<T> map ) {
        return new TLongObjectHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Byte,T> wrap( TByteObjectHashMap<T> map ) {
        return new TByteObjectHashMapDecorator<T>( map );
    }

    /**
     * Wrap the given map in a decorator that uses the standard {@link java.util.Map Map}
     * interface.
     */
    public static <T> Map<Short,T> wrap( TShortObjectHashMap<T> map ) {
        return new TShortObjectHashMapDecorator<T>( map );
    }


    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Double> wrap( TDoubleHashSet set ) {
        return new TDoubleHashSetDecorator( set );
    }

    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Float> wrap( TFloatHashSet set ) {
        return new TFloatHashSetDecorator( set );
    }

    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Integer> wrap( TIntHashSet set ) {
        return new TIntHashSetDecorator( set );
    }

    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Long> wrap( TLongHashSet set ) {
        return new TLongHashSetDecorator( set );
    }

    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Byte> wrap( TByteHashSet set ) {
        return new TByteHashSetDecorator( set );
    }

    /**
     * Wrap the given set in a decorator that uses the standard {@link java.util.Set Set}
     * interface.
     */
    public static Set<Short> wrap( TShortHashSet set ) {
        return new TShortHashSetDecorator( set );
    }
}