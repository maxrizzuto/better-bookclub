<script lang="ts">
	import { enhance } from '$app/forms';
	import { flip } from 'svelte/animate';
	import { fly } from 'svelte/transition';
	import type { PageProps } from './$types';
	import UserBook from '$lib/components/UserBook.svelte';
	import { onMount } from 'svelte';
	import { invalidateAll } from '$app/navigation';

	type Books = Array<{
		title: string;
		work_id: string;
		isbn13: string;
		shelf?: string;
		rating?: number;
		olid?: string;
	}>;

	type ShelfStats = {
		[key: string]: number;
	};

	type UserData = Record<
		string,
		{
			books: Books;
			shelves: ShelfStats;
		}
	>;

	// dict with username : {books: [], shelves: {}}

	const users: string[] = $state([]);
	const userData: UserData = $state({});

	let formSubmitted: boolean = $state(false);

	async function getUserBooks() {
		formSubmitted = true;
		const query = users.map((uname) => `user=${uname}`).join('&');
		const response = await fetch(`http://127.0.0.1:8000/users?${query}`);
		const data = await response.json();
		for (var user of data) {
			if (user.username in userData) {
				userData[user.username].books.push(...user.books);
				userData[user.username].shelves = {
					...userData[user.username].shelves,
					...user.shelves
				};
			} else {
				userData[user.username] = {
					books: user.books,
					shelves: user.shelves
				};
			}
		}
	}

	// [TODO] function for add user, export serverside function that checks user (not form action)
	function addUser() {
		const uname = (document.getElementById('uname') as HTMLFormElement).value.trim();
		const errorMessage = document.getElementById('inputError')!;
		errorMessage.textContent = '';
		if (!uname) {
			errorMessage.textContent = 'User input is empty.';
		} else if (users.includes(uname)) {
			errorMessage.textContent = 'User has already been added.';
		} else {
			users.unshift(uname);
		}
		(document.getElementById('uname')! as HTMLFormElement).value = '';
	}

	function removeUser(uname: string) {
		users.splice(users.indexOf(uname), 1);
	}

	function handleEnter(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			e.preventDefault();
			addUser();
		}
	}

	function checkSubmit(e: Event) {
		if (users.length < 2) {
			e.preventDefault();

			return;
		}
	}
</script>

<div id="page-container">
	{#if formSubmitted}
		<div class="users-container">
			{#if userData}
				{#each Object.entries(userData) as [username, data]}
					<div class="user">
						<div class="user-text">
							<h1 class="username">
								<a href="https://app.thestorygraph.com/profile/{username}">{username}</a>
							</h1>
							<div class="shelves">
								{#each Object.entries(data.shelves) as [shelf, number]}
									<p class="shelf"><b>{number}</b> {shelf.toLowerCase()}</p>
								{/each}
							</div>
						</div>
						<div class="user-books">
							{#each data.books as book}
								<div class="book-container">
									{#if book.olid !== null}
										<UserBook id={book.olid} />
									{:else}
										<UserBook id={book.isbn13} />
									{/if}
									<div class="rating-container">
										<div class="star-wrapper">
											<div class="css-star"></div>
										</div>
										<span class="rating">{book.rating}</span>
									</div>
								</div>
							{/each}
						</div>
					</div>
				{/each}
			{/if}
		</div>
		<form method="POST" action="?/getRecs">
			<button type="submit">Get recs &RightArrow;</button>
			{#each users as user (user)}
				<input type="hidden" name="user" value={user} />
			{/each}
		</form>

		<style>
			/* CONTAINERS */

			.book-container {
				display: flex;
				flex-direction: column;
				align-items: center;
				margin: 5px 20px;
			}

			.users-container {
				width: stretch;
			}

			button {
				margin: 0 0 0 5vw;
				font-size: 2em;
				background-color: antiquewhite;
			}

			.shelves {
				color: #b2c3e9;
				font-size: 1.5em;
			}

			.shelf {
				margin: 5px 0;
			}

			/* USERS */
			a {
				color: inherit;
				text-decoration: none;
			}

			.user-text {
				height: 100%;
				flex: 1;
				display: flex;
				flex-direction: column;
				align-items: flex-start;
				min-width: 250px;
			}

			.user {
				display: flex;
				flex-direction: row;
				justify-content: flex-start;
				border-bottom: 1px solid antiquewhite;
			}

			.username {
				font-size: 4em;
				font-weight: 100;
				margin: 10px 75px 0 0;
			}

			.username:hover {
				cursor: pointer;
				text-decoration: underline;
			}

			.user-books {
				display: flex;
				justify-content: flex-start;
				align-items: baseline;
				flex: 6;
				flex-wrap: wrap;
			}

			/*RATINGS*/
			.rating-container {
				display: flex;
				flex-direction: row;
				height: 40px;
				width: 100%;
				align-items: flex-end;
				justify-content: flex-start;
			}

			.rating {
				font-size: 2em;
			}

			.star-wrapper {
				width: 40px;
				height: 40px;
				flex-shrink: 0;
				margin-right: 2px;
			}

			.css-star {
				box-shadow:
					22px 10px 0 0 rgba(242, 191, 51, 1),
					20px 12px 0 0 rgba(242, 191, 51, 1),
					22px 12px 0 0 rgba(242, 191, 51, 1),
					24px 12px 0 0 rgba(242, 191, 51, 1),
					18px 14px 0 0 rgba(242, 191, 51, 1),
					20px 14px 0 0 rgba(242, 191, 51, 1),
					22px 14px 0 0 rgba(242, 191, 51, 1),
					24px 14px 0 0 rgba(242, 191, 51, 1),
					26px 14px 0 0 rgba(242, 191, 51, 1),
					18px 16px 0 0 rgba(242, 191, 51, 1),
					20px 16px 0 0 rgba(242, 191, 51, 1),
					22px 16px 0 0 rgba(242, 191, 51, 1),
					26px 16px 0 0 rgba(242, 191, 51, 1),
					12px 18px 0 0 rgba(242, 191, 51, 1),
					14px 18px 0 0 rgba(242, 191, 51, 1),
					16px 18px 0 0 rgba(242, 191, 51, 1),
					18px 18px 0 0 rgba(242, 191, 51, 1),
					20px 18px 0 0 rgba(242, 191, 51, 1),
					22px 18px 0 0 rgba(242, 191, 51, 1),
					26px 18px 0 0 rgba(242, 191, 51, 1),
					28px 18px 0 0 rgba(242, 191, 51, 1),
					30px 18px 0 0 rgba(242, 191, 51, 1),
					32px 18px 0 0 rgba(242, 191, 51, 1),
					10px 20px 0 0 rgba(242, 191, 51, 1),
					12px 20px 0 0 rgba(242, 191, 51, 1),
					14px 20px 0 0 rgba(242, 191, 51, 1),
					16px 20px 0 0 rgba(242, 191, 51, 1),
					18px 20px 0 0 rgba(242, 191, 51, 1),
					20px 20px 0 0 rgba(242, 191, 51, 1),
					22px 20px 0 0 rgba(242, 191, 51, 1),
					24px 20px 0 0 rgba(242, 191, 51, 1),
					28px 20px 0 0 rgba(242, 191, 51, 1),
					30px 20px 0 0 rgba(242, 191, 51, 1),
					32px 20px 0 0 rgba(242, 191, 51, 1),
					34px 20px 0 0 rgba(242, 191, 51, 1),
					12px 22px 0 0 rgba(242, 191, 51, 1),
					14px 22px 0 0 rgba(242, 191, 51, 1),
					16px 22px 0 0 rgba(242, 191, 51, 1),
					18px 22px 0 0 rgba(242, 191, 51, 1),
					20px 22px 0 0 rgba(242, 191, 51, 1),
					22px 22px 0 0 rgba(242, 191, 51, 1),
					24px 22px 0 0 rgba(242, 191, 51, 1),
					28px 22px 0 0 rgba(242, 191, 51, 1),
					30px 22px 0 0 rgba(242, 191, 51, 1),
					32px 22px 0 0 rgba(242, 191, 51, 1),
					16px 24px 0 0 rgba(242, 191, 51, 1),
					18px 24px 0 0 rgba(242, 191, 51, 1),
					20px 24px 0 0 rgba(242, 191, 51, 1),
					22px 24px 0 0 rgba(242, 191, 51, 1),
					24px 24px 0 0 rgba(242, 191, 51, 1),
					26px 24px 0 0 rgba(242, 191, 51, 1),
					28px 24px 0 0 rgba(242, 191, 51, 1),
					16px 26px 0 0 rgba(242, 191, 51, 1),
					18px 26px 0 0 rgba(242, 191, 51, 1),
					20px 26px 0 0 rgba(242, 191, 51, 1),
					22px 26px 0 0 rgba(242, 191, 51, 1),
					24px 26px 0 0 rgba(242, 191, 51, 1),
					26px 26px 0 0 rgba(242, 191, 51, 1),
					28px 26px 0 0 rgba(242, 191, 51, 1),
					14px 28px 0 0 rgba(242, 191, 51, 1),
					16px 28px 0 0 rgba(242, 191, 51, 1),
					18px 28px 0 0 rgba(242, 191, 51, 1),
					20px 28px 0 0 rgba(242, 191, 51, 1),
					24px 28px 0 0 rgba(242, 191, 51, 1),
					26px 28px 0 0 rgba(242, 191, 51, 1),
					28px 28px 0 0 rgba(242, 191, 51, 1),
					30px 28px 0 0 rgba(242, 191, 51, 1),
					14px 30px 0 0 rgba(242, 191, 51, 1),
					16px 30px 0 0 rgba(242, 191, 51, 1),
					18px 30px 0 0 rgba(242, 191, 51, 1),
					26px 30px 0 0 rgba(242, 191, 51, 1),
					28px 30px 0 0 rgba(242, 191, 51, 1),
					30px 30px 0 0 rgba(242, 191, 51, 1),
					16px 32px 0 0 rgba(242, 191, 51, 1),
					28px 32px 0 0 rgba(242, 191, 51, 1);
				height: 2px;
				width: 2px;
			}
		</style>
	{:else}
		<div class="content-container">
			<div id="title-block">
				<h1 id="title">Want ideas for your next book club book?</h1>
			</div>

			<div id="form">
				<form
					method="GET"
					name="unamesForm"
					id="unamesForm"
					onsubmit={(e) => {
						e.preventDefault();
						getUserBooks();
					}}
				>
					<div class="inputs">
						<input
							type="text"
							id="uname"
							placeholder="add StoryGraph usernames here"
							onkeydown={handleEnter}
						/>
						<button type="button" id="addUser" onclick={addUser}>+</button>
						<div class="submit-button">
							<button
								type="submit"
								id="submit"
								class:submittable={users.length >= 2}
								onclick={checkSubmit}
							>
								&RightArrow;
							</button>
							<span class="tooltip">Must add at least 2 users to submit</span>
						</div>
					</div>
					<div id="inputError"></div>
					<div id="users">
						{#each users as user (user)}
							<div class="user" id={user}>
								<!-- animate:flip={{ duration: 400 }} -->
								<!-- transition:fly={{ y: -5, duration: 400 }} -->
								<!-- [TODO] add validation icon if uname exists -->
								<input type="hidden" name="user" value={user} />
								<button class="removeUser" type="button" onclick={() => removeUser(user)}>-</button>
								<p>{user}</p>
							</div>
						{/each}
					</div>
				</form>
			</div>
		</div>

		<style>
			.content-container {
				margin: 15vh 10vw;
			}

			/* TITLE */
			#title-block {
				display: flex;
				align-items: flex-end;
				/*margin-left: 50vw;*/
				align-self: flex-start;
			}

			#title {
				font-weight: 100;
				font-size: 5rem;
				max-width: 70vw;
				text-align: flex-start;
				margin-bottom: 0;
			}

			/* FORM AND INPUTS */

			#form {
				display: flex;
				justify-content: flex-start;
				align-items: center;
				font-size: 1.25rem;
				padding-top: 1vh;
				margin-top: 3vh;
				width: 70vw;
				margin-left: 1vw;
			}

			#inputError {
				color: white;
				font-size: 0.75em;
				margin: 10px 20px;
				height: 0.75em;
			}

			input {
				width: 50vw;
				font-size: 1em;
				padding-left: 20px;
				max-width: 1000px;
				text-align: left;
				font-family: OCRA, sans-serif;
				height: 75px;
			}

			#addUser {
				display: inline-block;
				box-sizing: content-box;
				height: 50px;
				width: 50px;
				margin-left: 10px;
				font-size: 2em;
				background-color: black;
				color: antiquewhite;
				padding: 0px;
				border: 1px solid antiquewhite;
			}

			#addUser:hover {
				background-color: gray;
				cursor: pointer;
			}

			.inputs {
				display: flex;
				align-items: center;
			}

			#submit {
				display: inline-block;
				height: 50px;
				width: 50px;
				margin-left: 10px;
				background-color: gray;
				color: antiquewhite;
				border: 1px solid antiquewhite;
				padding: 0;
				font-size: 1.5em;
			}

			#submit.submittable {
				background-color: black;
			}

			.tooltip {
				font-size: 0.9em;
				visibility: hidden;
				margin-left: 10px;
			}

			#submit:hover + .tooltip {
				visibility: visible;
			}

			#submit.submittable + .tooltip {
				visibility: hidden;
			}

			#submit.submittable:hover {
				background-color: darkseagreen;
				cursor: pointer;
			}

			.submit-button {
				display: flex;
				align-items: center;
			}

			/* ADDED USERS */

			.removeUser {
				color: antiquewhite;
				background-color: black;
				font-size: 2em;
				width: 1em;
				height: 1em;
				font-size: 1.5em;
				margin-right: 20px;
				display: flex;
				align-items: center;
				justify-content: center;
				border: 1px solid antiquewhite;
			}

			.removeUser:hover {
				background-color: red;
				cursor: pointer;
			}

			#users {
				display: flex;
				padding-left: 5px;
				width: 90%;
				flex-direction: column;
				flex-wrap: wrap;
			}

			.user {
				flex: 1;
				display: flex;
				align-items: center;
				border-bottom: 1px solid antiquewhite;
				font-family: OCRA, sans-serif;
				font-size: 1.5em;
			}

			.user:last-of-type {
				padding-bottom: 0;
				border-bottom: none;
			}
		</style>
	{/if}
</div>

<!-- global styles -->
<style>
	/* OVERALL PAGE */
	#page-container {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		padding: 15vh 10vw;
	}
</style>
