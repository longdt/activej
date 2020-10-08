/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.serializer.annotations;

import io.activej.serializer.impl.SerializerDefBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public final class SerializeCustomHandler implements AnnotationHandler<SerializeCustom, SerializeCustomEx> {
	@Override
	public SerializerDefBuilder createBuilder(Context context, SerializeCustom annotation) {
		return (type, generics, target) -> {
			Class<? extends SerializerDefBuilder> handlerClass = annotation.value();
			try {
				Constructor<? extends SerializerDefBuilder> constructor = handlerClass.getDeclaredConstructor();
				SerializerDefBuilder serializerDefBuilder = constructor.newInstance();
				return serializerDefBuilder.serializer(type, generics, target);
			} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new AssertionError(e);
			}
		};
	}

	@Override
	public int[] extractPath(SerializeCustom annotation) {
		return annotation.path();
	}

	@Override
	public SerializeCustom[] extractList(SerializeCustomEx plural) {
		return plural.value();
	}
}
